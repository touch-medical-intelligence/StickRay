import asyncio
import atexit
import logging
from datetime import timedelta
from typing import Dict, Tuple, TypeVar, Type

import ray
from pydantic import confloat
from ray import ObjectRef
from ray.actor import ActorHandle, exit_actor
from ray.util.metrics import Gauge
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.consistent_hash_ring import ConsistentHashRing, NoAvailableNode, EmptyRing
from stick_ray.controller import StickRayController, WorkerEntry, ServiceNotFoundError
from stick_ray.eventbus import EventBus
from stick_ray.namespace import NAMESPACE
from stick_ray.utils import get_or_create_event_loop, loop_task, SerialisableBaseModel
from stick_ray.worker_proxy import WorkerProxy, HEARTBEAT_INTERVAL, WorkerUpdate, BackPressure, \
    SessionNotFound

__all__ = [
    'Router'
]

logger = logging.getLogger(__name__)

V = TypeVar('V')


class BaseRouter:
    def __init__(self, actor: ActorHandle):
        self._actor = actor

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return BaseRouter(**kwargs)

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        serialised_data = dict(
            actor=self._actor,
        )
        return self.__class__._deserialise, (serialised_data,)


class Router(BaseRouter):
    """
    A class representing a router. This is never invoked directly.
    """

    def __init__(self, node_id: str):
        router_name = self.router_actor_name(node_id=node_id)
        try:
            # Try to connect to existing router
            actor = ray.get_actor(router_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {router_name}")
        except ValueError:
            # Create new router
            router_actor_options = {
                "num_cpus": 0,
                "name": router_name,
                "lifetime": "detached",
                # No restart on failure. Another router will be created cleanly by controller.
                "max_restarts": 0,
                "max_task_retries": -1,
                # Schedule the controller on the given node.
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node_id, soft=True),
                "namespace": NAMESPACE,
                "max_concurrency": 15000  # Needs to be large, as there should be no limit.
            }
            dynamic_cls = self.dynamic_router_cls()
            actor = ray.remote(dynamic_cls).options(**router_actor_options).remote()
            ray.get(actor.health_check.remote())
            logger.info(f"Created new {router_name}")

        super().__init__(actor=actor)

    @classmethod
    def dynamic_router_cls(cls) -> Type:
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"StickRayRouter",
            (_Router,),
            dict(_Router.__dict__),
        )

    @classmethod
    def router_actor_name(cls, node_id: str) -> str:
        router_name = f"STICK_RAY_ROUTER#{node_id}"
        return router_name

    async def merge_services(self, services: Dict[str, Dict[str, WorkerProxy]]):
        """
        Sets the available services, and workers for a service.

        Args:
            services: services -> worker_id -> worker
        """
        await self._actor.set_services.remote(services=services)

    async def pause_service(self, service_name: str):
        """
        Pauses a service routing. Only called by controller.

        Args:
            service_name: the name of the service
        """
        await self._actor.pause_service.remote(service_name=service_name)

    async def unpause_service(self, service_name: str):
        """
        Unpauses a service routing. Only called by controller.

        Args:
            service_name: the name of the service
        """
        await self._actor.unpause_service.remote(service_name=service_name)

    async def set_workers(self, service_name: str, workers: Dict[str, WorkerProxy]):
        """
        Sets the available workers for a service.
        
        Args:
            service_name: service names
            workers: workers
        """
        await self._actor.set_workers.remote(service_name=service_name, workers=workers)

    async def ferry(self, service_name: str, method: str, session_id: str, data_ref_tuple: Tuple[ObjectRef]) \
            -> Tuple[ObjectRef]:
        """
        Forwards a request to the correct worker with the current session. If no session can be found, creates a new one.

        Args:
            service_name: the name of the routed service
            method: the name of the method to be ferried. Only methods not starting with '_' are permitted.
            session_id: the id of the session. No constraints on the shape of this string. Typically, a UUID.
            data_ref_tuple: a tuple of an object ref containing the args to the method. These are resolved on the other
                side, so that the ferry itself never touches the object.

        Returns:
            a tuple of object ref representing the results.

        Raises:
            RetryRouting: if routing should be retried.
            WorkerNotFoundError if worker not found
            ServiceNotFoundError if service not found
        """
        return await self._actor.ferry.remote(
            service_name=service_name,
            method=method,
            session_id=session_id,
            data_ref_tuple=data_ref_tuple
        )


class WorkerItem(SerialisableBaseModel):
    worker: WorkerProxy
    utilisation: confloat(ge=0)


class RetryRouting(Exception):
    pass


class WorkerNotFoundError(Exception):
    pass


class _Router:
    """
    A class representing a routed service. This is never invoked directly.
    """

    def __init__(self):

        self._router_lock = asyncio.Lock()  # for any manipulation of service registry, very little contention expected.
        self._services_lock = asyncio.Lock()  # Lock for changes the set of services
        self._workers_lock = asyncio.Lock()  # Lock for changing the set of workers per service
        self._service_locks: Dict[str, asyncio.Lock] = dict()  # service_name -> lock
        self._worker_entries: Dict[str, Dict[str, WorkerItem]] = dict()  # service_name -> worker_id -> worker_entry
        self._session_map: Dict[str, Dict[str, str]] = dict()  # service_name -> session_id -> worker_id
        self._rings: Dict[str, ConsistentHashRing] = dict()  # service_name -> ring
        self._active: Dict[str, asyncio.Event] = dict()  # service_name -> Event

        self.active_sessions = Gauge(
            'active_sessions',
            description="Measures how many sessions are live currently across all workers for a service.",
            tag_keys=('service_name',)
        )

        self._controller = StickRayController()
        self._event_bus = EventBus()

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

    async def _initialise(self):
        await self._run_control_loop()

    async def _run_control_loop(self):
        logger.info(f"Starting worker control loop for {self.__class__.__name__}!")
        while True:
            try:
                # We do the following:
                # 1. Log into
                # 2. Update metrics
                await asyncio.gather(
                    loop_task(task_fn=self._log_info, interval=timedelta(seconds=60)),
                    loop_task(task_fn=self._update_metrics, interval=timedelta(seconds=60)),
                    loop_task(task_fn=self._listen_to_gossip, interval=timedelta(seconds=60)),
                    return_exceptions=False
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def shutdown(self):
        """
        Shutdown the actor gracefully.
        """
        # Triggers atexit handlers
        exit_actor()

    async def pause_service(self, service_name: str):
        """
        Pauses a service.

        Args:
            service_name: the name of the service
        """
        async with self._services_lock:
            if service_name not in self._service_locks:
                raise ServiceNotFoundError(f"Service {service_name} not found.")
            async with self._service_locks[service_name]:
                self._active[service_name].clear()

    async def unpause_service(self, service_name: str):
        """
        Pauses a service.

        Args:
            service_name: the name of the service
        """
        async with self._services_lock:
            if service_name not in self._service_locks:
                raise ServiceNotFoundError(f"Service {service_name} not found.")
            async with self._service_locks[service_name]:
                self._active[service_name].set()

    async def update_workers(self, service_name: str, workers: Dict[str, WorkerEntry]):
        """
        Updates the workers for a service. The controller will call this.

        Args:
            service_name: the name of the service
            workers: the workers
        """
        async with self._services_lock:
            if service_name not in self._service_locks:
                raise ServiceNotFoundError(f"Service {service_name} not found.")
            async with self._service_locks[service_name]:
                self._worker_entries[service_name] = workers
                self._rings[service_name] = ConsistentHashRing()
                for worker_id in workers:
                    self._rings[service_name].add_node(node=worker_id, weight=1)

    async def health_check(self):
        """
        Announce health check.
        """
        return

    async def merge_services(self, services: Dict[str, Dict[str, WorkerProxy]]):
        """
        Sets the available services, and workers for a service.

        Args:
            services: services -> worker_id -> worker
        """
        for service_name in services:
            async with self._services_lock:
                if service_name in self._service_locks:
                    logger.warning(f"Service {service_name} already registered.")
                else:
                    self._session_map[service_name] = dict()
                    self._service_locks[service_name] = asyncio.Lock()
                    self._worker_entries[service_name] = dict()
                    self._rings[service_name] = ConsistentHashRing()
                    self._active[service_name] = asyncio.Event()
                async with self._service_locks[service_name]:
                    for worker_id, worker in services[service_name].items():
                        self._worker_entries[service_name][worker_id] = WorkerItem(
                            worker=worker,
                            utilisation=0.
                        )

    async def delete_service(self, service_name: str):
        """
        Deletes a service.

        Args:
            service_name: the name of the service
        """
        async with self._services_lock:
            if service_name not in self._service_locks:
                logger.warning(f"Service {service_name} not registered.")
                return
            async with self._service_locks[service_name]:
                del self._session_map[service_name]
                del self._worker_entries[service_name]
                del self._rings[service_name]
                del self._active[service_name]
                del self._service_locks[service_name]

    async def _log_info(self):
        """
        Logs information about the current routing table.
        """

        def count_sessions_per_worker(service_name: str) -> Dict[str, int]:
            sessions_per_worker: Dict[str, int] = dict()
            for session_id in self._session_map[service_name]:
                worker_id = self._session_map[service_name][session_id]
                if worker_id not in sessions_per_worker:
                    sessions_per_worker[worker_id] = 0
                sessions_per_worker[worker_id] += 1
            return sessions_per_worker

        def get_info(service_name: str) -> str:
            """
            Get information string of the current routing table.
            """
            info = f"Service: {service_name}\n"
            info += f"# Sessions aware of: {len(self._session_map[service_name]):04d}\n"
            for worker_id, session_count in count_sessions_per_worker(service_name=service_name):
                utilisation = self._worker_entries[service_name][worker_id].utilisation
                info += f"\t({utilisation * 100:0.2f}%) Worker {worker_id}: {session_count} sessions\n"
            return info

        async with self._service_locks:
            for service_name in self._session_map:
                if service_name not in self._session_map:
                    continue
                logger.info(get_info(service_name=service_name))

    async def _update_metrics(self):
        """
        Updates metrics, such as the number of active sessions per service.
        """
        while True:
            # Get the number of sessions per service
            async with self._router_lock:
                for service_name in self._session_map:
                    tags = {"service_name": service_name}
                    active_sessions = len(self._session_map[service_name])
                    self.active_sessions.set(active_sessions, tags=tags)
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _listen_to_gossip(self):
        """
        Reconciles the session map by reading the broadcasted updates from the workers.
        """
        async with self._services_lock:
            for service_name in self._worker_entries:
                # Skip paused services, because workers list ist being updated.
                if not self._active[service_name].is_set():
                    continue
                async with self._service_locks[service_name]:
                    for worker_id in self._worker_entries[service_name]:
                        # No-wait peek
                        worker_update: WorkerUpdate = await self._event_bus.peek(
                            key=f"{service_name}_{worker_id}",
                            timeout=0
                        )
                        # Update backpressure
                        self._worker_entries[service_name][worker_id].utilisation = worker_update.utilisation > 1.
                        # Update session map
                        for session_id in worker_update.session_ids:
                            self._session_map[service_name][session_id] = worker_id

    async def ferry(self, service_name: str, method: str, session_id: str, data_ref_tuple: Tuple[ObjectRef]) \
            -> Tuple[ObjectRef]:
        """
        Forwards a request to the correct worker with the current session. If no session can be found, creates a new one.

        Args:
            service_name: the name of the routed service
            method: the name of the method to be ferried. Only methods not starting with '_' are permitted.
            session_id: the id of the session. No constraints on the shape of this string. Typically, a UUID.
            data_ref_tuple: a tuple of an object ref containing the args to the method. These are resolved on the other
                side, so that the ferry itself never touches the object.

        Returns:
            a tuple of object ref representing the results.

        Raises:
            RetryRouting: routing should be retried
            WorkerNotFoundError if worker not found
            ServiceNotFoundError if service not found
        """

        # The reason we use this pattern is so that we can release the main lock as soon as the
        # service lock is acquired. In order for this to be robust all other modifications to the services must
        # follow the same pattern.
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

            # Start ferry

            # Wait until service is unpaused, as workers are being updated.
            await self._active[service_name].wait()
            try:
                for worker_id in self._rings[service_name].node_iter(session_id):
                    worker_entry = self._worker_entries[service_name][worker_id]
                    try:
                        (obj_ref,) = await worker_entry.worker.ferry(
                            method=method,
                            data_ref_tuple=data_ref_tuple,
                            session_id=session_id,
                            grant=False
                        )
                        return (obj_ref,)
                    except BackPressure:
                        # Try the next node
                        # Note: we don't rely on the gossiped utilisation, as it may be out of date.
                        continue
                    except SessionNotFound:
                        # Try to claim
                        granted_worker_id = await self._controller.stake_claim(
                            service_name=service_name,
                            worker_id=worker_id,
                            session_id=session_id
                        )
                        if granted_worker_id not in self._worker_entries[service_name]:
                            raise WorkerNotFoundError(
                                f"Worker {granted_worker_id} not found."
                            )
                        worker_entry = self._worker_entries[service_name][granted_worker_id]
                        (obj_ref,) = await worker_entry.worker.ferry(
                            method=method,
                            data_ref_tuple=data_ref_tuple,
                            session_id=session_id,
                            grant=True
                        )
                        return (obj_ref,)

            except (EmptyRing, NoAvailableNode):
                # Ask service to start up new worker
                await self._controller.request_scale_up(service_name=service_name)
                raise RetryRouting()
            # End ferry

        finally:
            if service_exists:
                if self._service_locks[service_name].locked():
                    self._service_locks[service_name].release()
