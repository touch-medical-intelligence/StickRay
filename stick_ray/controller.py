import asyncio
import atexit
import logging
from datetime import timedelta
from typing import Type, Dict, Any, Set
from uuid import uuid4

import ray
from pydantic import conint, validator, confloat
from ray.actor import ActorHandle, exit_actor
from ray.serve._private.utils import get_head_node_id
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.namespace import NAMESPACE
from stick_ray.router import Router
from stick_ray.stateful_worker import StatefulWorker
from stick_ray.utils import SerialisableBaseModel, current_utc, get_or_create_event_loop, loop_task
from stick_ray.worker_proxy import WorkerProxy

logger = logging.getLogger(__name__)


class ServiceNotFoundError(Exception):
    """
    An exception raised when a routed service is is not found regsitered with the router.
    """
    pass


class WorkerParams(SerialisableBaseModel):
    """
    A class representing a worker startup entry.
    """
    worker_cls: Type[StatefulWorker]
    worker_actor_options: Dict[str, Any]
    worker_kwargs: Dict[str, Any]
    max_concurrent_sessions: conint(ge=1)
    expiry_period: timedelta

    @validator('expiry_period')
    def expiry_period_must_be_positive(cls, v):
        if v <= timedelta(seconds=0):
            raise ValueError("Expiry period must be positive.")
        return v


class ServiceParams(SerialisableBaseModel):
    """
    A class representing a worker startup entry.
    """
    worker_params: WorkerParams
    min_num_workers: conint(ge=0)


class WorkerEntry(SerialisableBaseModel):
    """
    A class representing a worker entry in the registry.
    """
    worker: WorkerProxy
    backpressure: bool = False
    utilisation: confloat(ge=0.)
    lazy_downscale: bool = False


class BaseStickRayController:
    def __init__(self, actor: ActorHandle):
        self._actor = actor

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return BaseStickRayController(**kwargs)

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        serialised_data = dict(
            actor=self._actor,
        )
        return self.__class__._deserialise, (serialised_data,)


class StickRayController(BaseStickRayController):
    """
    The Stick Ray controller.
    """

    def __init__(self):
        actor_name = self.actor_name()

        try:
            actor = ray.get_actor(actor_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {actor_name}")
        except ValueError:
            try:
                placement_node_id = get_head_node_id()
            except:
                placement_node_id = ray.get_runtime_context().get_node_id()
            actor_options = {
                "num_cpus": 0,
                "name": actor_name,
                "lifetime": "detached",
                "max_restarts": -1,
                "max_task_retries": -1,
                # Schedule the controller on the head node with a soft constraint. This
                # prefers it to run on the head node in most cases, but allows it to be
                # restarted on other nodes in an HA cluster.
                "scheduling_strategy": NodeAffinitySchedulingStrategy(placement_node_id, soft=True),
                "namespace": NAMESPACE,
                "max_concurrency": 15000  # Needs to be large, as there should be no limit.
            }

            dynamic_cls = self.dynamic_cls()

            actor = ray.remote(dynamic_cls).options(**actor_options).remote()
            ray.get(actor.health_check.remote())
            logger.info(f"Created new {actor_name}")
        super().__init__(actor=actor)

    @staticmethod
    def dynamic_cls() -> Type:
        """
        Create a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.

        Args:
            name: name of the event bus

        Returns:
            a dynamic class
        """
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"StickRayController",
            (_StickRayController,),
            dict(_StickRayController.__dict__),
        )

    @staticmethod
    def actor_name() -> str:
        return f"STICK_RAY_CONTROLLER_ACTOR"

    async def add_service(self, service_name: str, service_params: ServiceParams):
        """
        Add a service to the registry.

        Args:
            service_name: name of the service
            service_params: ServiceParams for the service
        """
        return self._actor.add_service.remote(service_name, service_params)

    async def delete_service(self, service_name: str):
        """
        Delete a service from the registry.

        Args:
            service_name: name of the service
        """
        return self._actor.delete_service.remote(service_name)

    async def get_service_params(self, service_name: str) -> ServiceParams:
        """
        Get the service params for a service.

        Args:
            service_name: name of the service

        Returns:
            ServiceParams for the service
        """
        return await self._actor.get_service_params.remote(service_name)

    async def get_services(self) -> Set[str]:
        ...

    async def get_registry(self) -> Dict[str, ServiceParams]:
        """
        Get the registry.

        Returns:
            Dict of service name to ServiceParams

        Raises:
            ValueError if service not found
        """
        return await self._actor.get_registry.remote()

    async def request_scale_up(self, service_name: str):
        """
        Request scale-up of service. Returns immediately.

        Args:
            service_name: service name
        """
        return await self._actor.add_new_worker.remote(service_name=service_name)

    async def stake_claim(self, service_name: str, worker_id: str, session_id: str):
        """
        Stakes a claim for a session, which is granted or denied. This is called when a router wants to session a
        session request to a worker, and the session does not already exist there.

        Args:
            service_name: service name
            stake_worker_id: worker id
            session_id: session id

        Returns:
            the worker id where the claim is at. Returns `worker_id` iff the stake was granted.
        """
        return await self._actor.stake_claim.remote(
            service_name=service_name,
            worker_id=worker_id,
            session_id=session_id
        )

    def get_router(self, node_id: str) -> Router:
        """
        Gets a router for the given node.

        Args:
            node_id: node id to get router for

        Returns:
            Router on the node
        """
        return ray.get(self._actor.get_router.remote(node_id=node_id))

    def register_service(self, service_name: str, service_params: ServiceParams):
        """
        Add a service to the registry.

        Args:
            service_name: name of the service
            service_params: ServiceParams for the service
        """
        return ray.get(self._actor.register_service.remote(service_name=service_name, service_params=service_params))


class _StickRayController:
    """
    The Stick Ray controller actor.
    """

    def __init__(self):
        self._routers_lock = asyncio.Lock()
        self._services_lock = asyncio.Lock()
        self._service_locks: Dict[str, asyncio.Lock] = dict()  # service_name -> asyncio.Lock
        self._service_params: Dict[str, ServiceParams] = dict()  # service_name -> ServiceParams
        self._workers: Dict[str, Dict[str, WorkerEntry]] = dict()  # service_name -> worker_id -> WorkerEntry
        self._routers: Dict[str, Router] = dict()  # node_id -> Router
        self._ledger: Dict[str, Dict[str, str]] = dict()  # service_name -> session_id -> worker_id
        self._scale_up_requests: Set[str] = set()  # service_name

        # Start control loop on actor creation, and assign clean up atexit
        self._control_long_running_task: asyncio.Task | None = get_or_create_event_loop().create_task(
            self._initialise()
        )

        atexit.register(self._atexit_clean_up())

    def _atexit_clean_up(self):

        # Stop control loop
        if self._control_long_running_task is not None:
            loop = get_or_create_event_loop()
            try:
                self._control_long_running_task.cancel()
                loop.run_until_complete(self._control_long_running_task)
            except asyncio.CancelledError:
                logger.info("Successfully cancelled control loop.")
            except Exception as e:
                logger.error(f"Problem stopping control loop. {str(e)}")
                raise
            finally:
                loop.close()

    async def shutdown(self):
        """
        Shutdown the actor gracefully.
        """
        # Triggers atexit handlers
        exit_actor()

    async def _initialise(self):
        await self._run_control_loop()

    async def _run_control_loop(self):
        logger.info(f"Starting worker control loop for {self.__class__.__name__}!")
        while True:
            try:
                await asyncio.gather(
                    loop_task(task_fn=self._auto_scale, interval=timedelta(seconds=1)),
                    return_exceptions=False
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def health_check(self):
        return

    async def _update_router(self, node_id: str):
        """
        Updates a router with the current state of the registry.

        Args:
            node_id: node id of the router to update

        Returns:
            Router on the node
        """
        # Make sure router is aware of all services.
        async with await self._services_lock:
            update: Dict[str, Dict[str, WorkerProxy]] = dict()
            for service_name in self._service_locks:
                async with self._service_locks[service_name]:
                    update[service_name] = dict(
                        (worker_id, worker_entry.worker)
                        for worker_id, worker_entry in self._workers[service_name].items()
                    )
        await self._routers[node_id].merge_services(services=update)

    async def get_router(self, node_id: str) -> Router:
        """
        Gets a router for the given node.

        Args:
            node_id: node id to get router for

        Returns:
            Router on the node
        """
        async with self._routers_lock:
            if node_id not in self._routers:
                # Even if the _routers dict is wrong, this will be okay because the router will be pull from object
                # store if it already exists (and a reference is held somewhere).
                self._routers[node_id] = Router(node_id=node_id)
                await self._update_router(node_id=node_id)
            return self._routers[node_id]

    def add_service(self, service_name: str, service_params: ServiceParams):
        """
        Add a service to the registry.

        Args:
            service_name: name of the service
            service_params: ServiceParams for the service
        """
        async with self._services_lock:
            if service_name not in self._workers:
                self._workers[service_name] = dict()
                self._service_locks[service_name] = asyncio.Lock()
                self._ledger[service_name] = dict()
            self._service_params[service_name] = service_params
            async with self._routers_lock:
                for node_id in self._routers:
                    await self._update_router(node_id=node_id)

    def delete_service(self, service_name: str):
        """
        Delete a service from the registry.

        Args:
            service_name: name of the service
        """
        async with self._routers_lock:
            if service_name not in self._service_params:
                logger.warning(f"Service {service_name} not found in registry.")
            else:
                del self._service_params[service_name]
            if service_name not in self._workers:
                logger.warning(f"Service {service_name} not found in registry.")
            else:
                del self._workers[service_name]

    def get_service_params(self, service_name: str) -> ServiceParams:
        """
        Get the service params for a service.

        Args:
            service_name: name of the service

        Returns:
            ServiceParams for the service

        Raises:
            ValueError if service not found
        """
        async with self._routers_lock:
            if service_name not in self._service_params:
                raise ValueError(f"Service {service_name} not found in registry.")
            return self._service_params[service_name]

    def get_registry(self) -> Dict[str, ServiceParams]:
        """
        Get the registry.

        Returns:
            Dict of service name to ServiceParams
        """
        return self._service_params

    def _is_service(self, service_name: str) -> bool:
        """
        Returns True iff the service is in the registry.

        Args:
            service_name: name of the service

        Returns:
            True iff the service is in the registry
        """
        if (service_name not in self._service_params) or (service_name not in self._workers):
            return False
        return True

    async def request_scale_up(self, service_name: str):
        """
        Add a worker to the service, handling router updates.

        Args:
            service_name: service name
        """
        async with self._routers_lock:
            if not self._is_service(service_name=service_name):
                raise ServiceNotFoundError(f"Service {service_name} not found in registry.")
            # Pause routing on this service.
            for router in self._routers.values():
                await router.pause_service(service_name=service_name)
            # Create a new worker.
            worker_id = await self._create_new_worker(service_name=service_name)
            # Update all routers.
            for router in self._routers.values():
                await router.unpause_service(service_name=service_name)
            # Unpause routing on this service.
            for router in self._routers.values():
                await router.unpause_service(service_name=service_name)

    @staticmethod
    def _dynamic_worker_actor_cls(worker_cls: Type, service_name: str) -> Type:
        """
        Create a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.

        Args:
            worker_cls: the worker class
            service_name: the name of the service

        Returns:
            a dynamic class
        """
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"StickRayReplica:{service_name}",
            (worker_cls,),
            dict(worker_cls.__dict__),
        )

    @staticmethod
    def _worker_actor_name(service_name: str, worker_id: str) -> str:
        """
        Get the name of the worker actor.

        Args:
            service_name: service name
            worker_id: worker id

        Returns:
            the name of the worker actor
        """
        # Low chance of collision.
        return f"STICK_RAY_REPLICA:{service_name}#{worker_id[:8]}"

    async def _shutdown_empty_workers(self):

        if len(self._worker_entries) == 0:
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

        for worker_id in list(self._worker_entries.keys()):
            await asyncio.sleep(0)
            async with self._router_lock:
                if worker_id not in self._worker_entries:  # shutdown before getting lock
                    continue
                if len(self._worker_entries) > self.min_num_workers:
                    count = self._count_worker_sessions(worker_id=worker_id)
                    if count == 0:
                        logger.info(f"Worker {worker_id} empty. Closing down.")
                        try:
                            await self._worker_entries[worker_id].worker.shutdown.remote()
                        except Exception as e:
                            logger.exception(str(e))
                        finally:
                            # Note, any existing references will prevent shutdown of actor until released.
                            del self._worker_entries[worker_id]

    async def _create_new_worker(self, service_name: str) -> str:
        """
        Creates a new worker for a service.

        Args:
            service_name: service name

        Returns:
            the new worker id
        """
        # We create a new worker actor, and then start it.
        service_params = self._service_params[service_name]
        worker_id = str(uuid4())
        while worker_id in self._workers[service_name]:  # Ensure no collision
            worker_id = str(uuid4())
        worker_actor_name = self._worker_actor_name(service_name=service_name, worker_id=worker_id)
        dynamic_cls = self._dynamic_worker_actor_cls(
            worker_cls=service_params.worker_params.worker_cls,
            service_name=service_name
        )

        worker_actor_options = service_params.worker_actor_options.copy()
        worker_actor_options['name'] = worker_actor_name
        if worker_actor_options.pop('lifetime', None):  # make sure we don't accidentally make a detached one
            logger.warning(f"Bad pattern: do not set `lifetime=detached` for {service_name}.")
        new_worker = ray.remote(dynamic_cls).options(**worker_actor_options).remote(**service_params.worker_kwargs)
        # Start the worker up.
        await new_worker.start.remote(worker_id=worker_id, service_name=service_name)
        logger.info(f"Started {worker_actor_name}")
        self._workers[service_name][worker_id] = WorkerEntry(
            worker_actor=new_worker,
            last_add_dt=current_utc() - timedelta(days=10000),
            backpressure=False,
            lazy_downscale=False
        )
        return worker_id

    async def _auto_scale(self):
        # The goal of auto-scaling is to all workers are running at capacity, but not over-capacity.

        for service_name in list(self._workers.keys()):
            service_exists = False
            try:
                await self._services_lock.acquire()
                try:
                    # Check if service is registered.
                    if service_name not in self._service_locks:
                        continue
                    service_exists = True
                    # Block until service is unpaused.
                    await self._service_locks[service_name].acquire()
                finally:
                    self._services_lock.release()

                # Start

                # First decide if we should scale up
                # For now that means,
                # 1. all active workers are under back-pressure,
                # 2. or, there are less than the designated minimum number of workers,
                # 3. or, there is a specific request.
                scale_up = False

                if len(self._workers[service_name]) < self._service_params[service_name].min_num_workers:
                    scale_up = True

                if all(worker.backpressure or worker.lazy_downscale for worker_id, worker in
                       self._workers[service_name].items()):
                    scale_up = True

                if service_name in self._scale_up_requests:
                    self._scale_up_requests.remove(service_name)
                    scale_up = True

                if scale_up:
                    # First if possible we reactivate a lazily downscaled worker.
                    # Choose the one with most capacity remaining
                    reactive_worker = sorted(
                        filter(lambda worker: worker.lazy_downscale and (not worker.utilisation),
                               self._workers[service_name].values()),
                        key=lambda worker: worker.utilisation
                    )
                    if len(reactive_worker) > 0:
                        reactive_worker = reactive_worker[0]
                        await reactive_worker.worker.unmark_lazy_downscale()
                        logger.info(f"Worker {reactive_worker.worker_id} reactivated.")

                # Now decide about scale down.
                # For this, if any node is under back-pressure, we don't scale down.

                scale_down = False
                if not scale_up:
                    for worker_id, worker in self._workers[service_name].items():
                        if worker.backpressure:
                            scale_down = False
                            break
                        if worker.lazy_downscale:
                            scale_down = True
                            break

                for worker_id, worker in self._workers[service_name].items():

                    if worker.lazy_downscale:  # If lazy downscale, downscale if possible.

                        continue
                    if worker.backpressure:
                        continue
                    if worker.lazy_downscale:
                        continue
                    if worker.last_add_dt + self._service_params[service_name].expiry_period < current_utc():
                        worker.lazy_downscale = True
                        await worker.worker.shutdown.remote()
                        del self._workers[service_name][worker.worker_id]
                        logger.info(f"Worker {worker.worker_id} expired. Closing down.")
                        continue
                    if len(self._workers[service_name]) < self._service_params[service_name].min_num_workers:
                        await self._create_new_worker(service_name=service_name)
                        continue
                    if len(self._workers[service_name]) > self._service_params[service_name].min_num_workers:
                        count = self._count_worker_sessions(worker_id=worker.worker_id)
                        if count == 0:
                            worker.lazy_downscale = True
                            await worker.worker.shutdown.remote()
                            del self._workers[service_name][worker.worker_id]
                            logger.info(f"Worker {worker.worker_id} empty. Closing down.")
                            continue
                # End

            finally:
                if service_exists:
                    if self._service_locks[service_name].locked():
                        self._service_locks[service_name].release()

    async def stake_claim(self, service_name: str, worker_id: str, session_id: str):
        """
        Stakes a claim for a session, which is granted or denied. This is called when a router wants to session a
        session request to a worker, and the session does not already exist there.

        Args:
            service_name: service name
            worker_id: worker id
            session_id: session id

        Returns:
            the worker id where the session is claimed. Returns `worker_id` iff the stake was granted.
        """
        service_exists = False
        try:
            await self._services_lock.acquire()
            try:
                # Check if service is registered.
                if service_name not in self._service_locks:
                    raise ServiceNotFoundError(service_name)
                service_exists = True
                # Block until service is unpaused.
                await self._service_locks[service_name].acquire()
            finally:
                self._services_lock.release()

            # Start
            if session_id not in self._ledger[service_name]:
                self._ledger[service_name][session_id] = worker_id
            return self._ledger[service_name][session_id]
            # End

        finally:
            if service_exists:
                if self._service_locks[service_name].locked():
                    self._service_locks[service_name].release()

    async def request_scale_up(self, service_name: str):
        """
        Make a request to scale up the service. Short circuits auto-scaler interval.

        Args:
            service_name: service to scale up
        """
        service_exists = False
        try:
            await self._services_lock.acquire()
            try:
                # Check if service is registered.
                if service_name not in self._ledger:
                    raise ServiceNotFoundError(service_name)
                service_exists = True
                # Block until service is unpaused.
                await self._service_locks[service_name].acquire()
            finally:
                self._services_lock.release()

            # Start
            self._scale_up_requests.add(service_name)
            # End

        finally:
            if service_exists:
                if self._service_locks[service_name].locked():
                    self._service_locks[service_name].release()
