import asyncio
import atexit
import logging
import pickle
from datetime import timedelta
from typing import Type, Dict, Set
from uuid import uuid4

import numpy as np
import ray
from pydantic import confloat
from ray.actor import exit_actor
from ray.serve._private.storage.kv_store import RayInternalKVStore, KVStoreError
from ray.serve._private.utils import get_head_node_id
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.abc import AbstractStickRayController, ServiceParams
from stick_ray.actor_interface import ActorInterface
from stick_ray.common import ServiceNotFoundError, LedgerEntry, WorkerUpdate, WorkerStillBusyError, \
    SerialisableBaseModel
from stick_ray.eventbus import EventBus
from stick_ray.namespace import NAMESPACE
from stick_ray.router import Router
from stick_ray.utils import current_utc, get_or_create_event_loop, loop_task
from stick_ray.worker_proxy import WorkerProxy

__all__ = [
    'StickRayController'
]

logger = logging.getLogger(__name__)


class WorkerEntry(SerialisableBaseModel):
    """
    A class representing a worker entry in the registry.
    """
    worker: WorkerProxy
    utilisation: confloat(ge=0.)
    lazy_downscale: bool = False


class ControllerRecoveryState(SerialisableBaseModel):
    """
    A class representing the recovery state of the controller.
    """
    workers: Dict[str, Dict[str, WorkerEntry]]
    routers: Dict[str, Router]
    service_params: Dict[str, ServiceParams]
    ledger: Dict[str, Dict[str, LedgerEntry]]  # service_name -> session_id -> LedgerEntry


class StickRayController(AbstractStickRayController, ActorInterface):
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

        ActorInterface.__init__(self, actor=actor)

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


class _StickRayController(AbstractStickRayController):
    """
    The Stick Ray controller actor.
    """

    _checkpoint_key = "STICK_RAY_CONTROLLER_CHECKPOINT"
    _metric_window = timedelta(seconds=60)
    _tune_gossip_interval = timedelta(seconds=60)
    _auto_scale_interval = timedelta(seconds=10)
    _save_interval = timedelta(seconds=60)
    _update_interval = timedelta(seconds=10)

    def __init__(self):
        self._kv_store = RayInternalKVStore(namespace=NAMESPACE)
        try:
            self._safe_init()
        except Exception as e:
            logger.exception(str(e))
            raise e
        # Start control loop on actor creation, and assign clean up atexit
        self._control_long_running_task: asyncio.Task | None = get_or_create_event_loop().create_task(
            self._run_control_loop()
        )
        atexit.register(self._atexit_clean_up)

    def _atexit_clean_up(self):

        # TODO: Stop all routers, workers, and services.
        async def clean_up():
            if self._control_long_running_task is not None:
                self._control_long_running_task.cancel()
                await self._control_long_running_task

        # Stop control loop
        loop = get_or_create_event_loop()
        try:
            loop.run_until_complete(clean_up())
        except asyncio.CancelledError:
            logger.info("Successfully cancelled control loop.")
        except Exception as e:
            logger.error(f"Problem stopping control loop. {str(e)}")
            raise
        finally:
            loop.close()
            ...

    async def _save_state(self):
        """
        Save state to object store.
        """
        controller_recovery_state = ControllerRecoveryState(
            workers=self._workers,
            routers=self._routers,
            service_params=self._service_params,
            ledger=self._ledger
        )
        state_bytes = pickle.dumps(controller_recovery_state)
        self._kv_store.put(key=self._checkpoint_key, val=state_bytes)

    def _safe_init(self):
        """
        Safely recover state from object store, or else create fresh state.
        """
        self._routers_lock = asyncio.Lock()
        self._services_lock = asyncio.Lock()
        self._service_locks: Dict[str, asyncio.Lock] = dict()  # service_name -> asyncio.Lock
        self._scale_up_requests: Set[str] = set()  # service_name
        self._event_bus = EventBus()
        try:
            state_bytes = self._kv_store.get(key=self._checkpoint_key)
            if state_bytes is None:
                raise ValueError
            controller_recovery_state: ControllerRecoveryState = pickle.loads(state_bytes)
            self._workers = controller_recovery_state.workers
            self._routers = controller_recovery_state.routers
            self._ledger = controller_recovery_state.ledger
            self._service_params = controller_recovery_state.service_params
            for service_name in self._service_params:
                self._service_locks[service_name] = asyncio.Lock()
        except (KVStoreError, ValueError):
            logger.info("Empty init")
            self._service_params: Dict[str, ServiceParams] = dict()  # service_name -> ServiceParams
            self._workers: Dict[str, Dict[str, WorkerEntry]] = dict()  # service_name -> worker_id -> WorkerEntry
            self._routers: Dict[str, Router] = dict()  # node_id -> Router
            self._ledger: Dict[str, Dict[str, LedgerEntry]] = dict()  # service_name -> session_id -> LedgerEntry

    async def _run_control_loop(self):
        logger.info(f"Starting control loop for {self.__class__.__name__}!")
        while True:
            try:
                await asyncio.gather(
                    loop_task(task_fn=self._auto_scale, interval=self._auto_scale_interval),
                    loop_task(task_fn=self._save_state, interval=self._save_interval),
                    loop_task(task_fn=self._listen_to_gossip, interval=self._update_interval),
                    loop_task(task_fn=self._tune_gossip, interval=self._tune_gossip_interval),
                    return_exceptions=False
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def _listen_to_gossip(self):
        """
        Reconciles the session map by reading the broadcasted updates from the workers.
        """
        async with self._services_lock:
            for service_name in self._workers:
                async with self._service_locks[service_name]:
                    for worker_id in self._workers[service_name]:
                        # No-wait peek
                        try:
                            worker_update: WorkerUpdate = await self._event_bus.peek(
                                key=f"{service_name}_{worker_id}",
                                timeout=0
                            )
                        except asyncio.TimeoutError:
                            continue
                        # Update backpressure
                        self._workers[service_name][worker_id].utilisation = worker_update.utilisation
                        # Update session map
                        for session_id in worker_update.session_ids:
                            if session_id not in self._ledger[service_name]:
                                # make sure it's in there.
                                self._ledger[service_name][session_id] = LedgerEntry(
                                    worker_id=worker_id,
                                    added_dt=current_utc()
                                )
                            else:
                                # If it's already in there, we check if it's the same worker.
                                if self._ledger[service_name][session_id].worker_id != worker_id:
                                    self._ledger[service_name][session_id].worker_id = worker_id
                                    self._ledger[service_name][session_id].added_dt = current_utc()

    async def health_check(self):
        return

    async def shutdown(self):
        # Triggers atexit handlers
        exit_actor()

    async def get_router(self, node_id: str) -> Router:
        async with self._routers_lock:
            if node_id not in self._routers:
                # Even if the _routers dict is wrong, this will be okay because the router will be pull from object
                # store if it already exists (and a reference is held somewhere).
                controller = StickRayController()  # Will get itself safely
                self._routers[node_id] = Router(node_id=node_id, controller=controller)
                # No need to pause because the router is not yet registered.
                await self._update_router(node_id=node_id)
            return self._routers[node_id]

    async def _update_router(self, node_id: str, service_name: str | None = None):
        """
        Updates a router with the current state of the registry.

        Args:
            node_id: node id of the router to update
            service_name: service name to update. If None, all services are updated.
        """

        if not self._routers_lock.locked():
            raise RuntimeError("Must acquire _routers_lock before calling _update_router.")

        # Make sure router is aware of all services.
        async with await self._services_lock:
            update: Dict[str, Dict[str, WorkerProxy]] = dict()
            if service_name is not None:
                async with self._service_locks[service_name]:
                    update[service_name] = dict(
                        (worker_id, worker_entry.worker)
                        for worker_id, worker_entry in self._workers[service_name].items()
                    )
            else:
                for service_name in self._service_locks:
                    async with self._service_locks[service_name]:
                        update[service_name] = dict(
                            (worker_id, worker_entry.worker)
                            for worker_id, worker_entry in self._workers[service_name].items()
                        )
        await self._routers[node_id].set_services(services=update)

    async def add_service(self, service_name: str, service_params: ServiceParams):
        async with self._services_lock:
            if service_name not in self._workers:
                self._workers[service_name] = dict()
                self._service_locks[service_name] = asyncio.Lock()
                self._ledger[service_name] = dict()
            # Update params regardless
            self._service_params[service_name] = service_params
            async with self._routers_lock:
                tasks = []
                for node_id in self._routers:
                    tasks.append(self._update_router(node_id=node_id, service_name=service_name))
                await asyncio.gather(*tasks, return_exceptions=False)

    async def delete_service(self, service_name: str):
        async with self._services_lock:
            if service_name not in self._service_locks:
                logger.warning(f"Service {service_name} not found in registry.")
                return

            async with self._service_locks[service_name]:
                del self._workers[service_name]
                del self._ledger[service_name]
                del self._service_params[service_name]
                del self._service_locks[service_name]

            async with self._routers_lock:
                tasks = []
                for node_id in self._routers:
                    tasks.append(self._routers[node_id].delete_service(service_name=service_name))
                await asyncio.gather(*tasks, return_exceptions=False)

    async def _create_new_worker(self, service_name: str) -> str:
        """
        Creates a new worker for a service.

        Args:
            service_name: service name

        Returns:
            the new worker id
        """
        if not self._service_locks[service_name].locked():
            raise RuntimeError("Must acquire _service_locks[service_name] before calling _create_new_worker.")

        # We create a new worker actor, and then start it.
        service_params = self._service_params[service_name]
        worker_id = str(uuid4())
        while worker_id in self._workers[service_name]:  # Ensure no collision
            worker_id = str(uuid4())
        worker = WorkerProxy(service_name=service_name, worker_id=worker_id, worker_params=service_params.worker_params)
        self._workers[service_name][worker_id] = WorkerEntry(
            worker=worker,
            utilisation=0.,
            lazy_downscale=False
        )
        # We pause all routing on this service, then update, then unpause all.
        async with self._routers_lock:
            tasks = []
            for node_id in self._routers:
                tasks.append(self._routers[node_id].pause_service(service_name=service_name))
            await asyncio.gather(*tasks, return_exceptions=False)
            tasks = []
            for node_id in self._routers:
                tasks.append(self._update_router(node_id=node_id, service_name=service_name))
            await asyncio.gather(*tasks, return_exceptions=False)
            tasks = []
            for node_id in self._routers:
                tasks.append(self._routers[node_id].unpause_service(service_name=service_name))
            await asyncio.gather(*tasks, return_exceptions=False)

        # Update the router.
        return worker_id

    async def _shutdown_worker(self, service_name: str, worker_id: str):
        """
        Shuts down a worker for a service.

        Args:
            service_name: service name
            worker_id: worker id
        """
        if not self._service_locks[service_name].locked():
            raise RuntimeError("Must acquire _service_locks[service_name] before calling _shutdown_worker.")
        worker = self._workers[service_name][worker_id].worker
        # Try to shutdown. If it's not empty, it will give error.
        try:
            await worker.shutdown()
            # We pause all routing on this service, then update, then unpause all.
            async with self._routers_lock:
                tasks = []
                for node_id in self._routers:
                    tasks.append(self._routers[node_id].pause_service(service_name=service_name))
                await asyncio.gather(*tasks, return_exceptions=False)
                tasks = []
                for node_id in self._routers:
                    tasks.append(self._update_router(node_id=node_id, service_name=service_name))
                await asyncio.gather(*tasks, return_exceptions=False)
                tasks = []
                for node_id in self._routers:
                    tasks.append(self._routers[node_id].unpause_service(service_name=service_name))
                await asyncio.gather(*tasks, return_exceptions=False)
        except WorkerStillBusyError:
            pass

    async def _tune_gossip(self):
        """
        Tunes the gossip interval based on the rate of new session creation.
        """
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
                # 1. all active workers are at,
                # 2. or, there are less than the designated minimum number of workers,
                # 3. or, there is a specific request.
                now = current_utc()
                count_in_window = len(
                    list(filter(lambda ledger_entry: (now - ledger_entry.added_dt) < self._metric_window,
                                self._ledger[service_name])))
                tau = self._metric_window / count_in_window
                # Choose T s.t. 1 - e^(-T/tau) is small
                prob_of_missing = 0.01
                # Lower bound of T is 0.5 seconds
                T = max(
                    -np.log(1 - prob_of_missing) * tau,
                    timedelta(seconds=0.5)
                )
                for worker_id, worker in self._workers[service_name].items():
                    await worker.worker.set_gossip_interval(interval=T)
                for router_id, router in self._routers.items():
                    await router.set_gossip_interval(service_name=service_name, interval=T)
                # End

            finally:
                if service_exists:
                    if self._service_locks[service_name].locked():
                        self._service_locks[service_name].release()

    async def _auto_scale(self):
        # The goal of auto-scaling is to all workers are running at capacity, but not over-capacity.
        # We always want to be able to handle the number of new sessions that are expected to arrive in the next
        # update period.

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
                # 1. all active workers are at,
                # 2. or, there are less than the designated minimum number of workers,
                # 3. or, there is a specific request.
                now = current_utc()
                count_in_window = len(
                    list(filter(lambda ledger_entry: (now - ledger_entry.added_dt) < self._metric_window,
                                self._ledger[service_name])))
                expected_new_workers = count_in_window * (self._auto_scale_interval / self._metric_window)
                N = self._service_params[service_name].worker_params.max_concurrent_sessions
                # Determine the number of workers needed to handle the expected number of new sessions.
                while True:
                    capacity = 0
                    for worker_id in self._workers[service_name]:
                        if not self._workers[service_name][worker_id].lazy_downscale:
                            capacity += (1 - self._workers[service_name][worker_id].utilisation) * N
                    if capacity < expected_new_workers:
                        # scale up one node, a lazy one that's least utilised, or a new one.
                        reactive_worker_ids = sorted(
                            filter(lambda worker_id: self._workers[service_name][worker_id].lazy_downscale,
                                   self._workers[service_name].keys()),
                            key=lambda worker_id: self._workers[service_name][worker_id].utilisation
                        )
                        if len(reactive_worker_ids) > 0:
                            reactive_worker_id = reactive_worker_ids[0]
                            await self._workers[service_name][reactive_worker_id].worker.unmark_lazy_downscale()
                            self._workers[service_name][reactive_worker_id].lazy_downscale = False
                            logger.info(f"Worker {reactive_worker_id} reactivated for {service_name}.")
                        else:
                            new_worker_id = await self._create_new_worker(service_name=service_name)
                            logger.info(f"Worker {new_worker_id} created for {service_name}.")
                        continue
                    else:
                        # We're above the threshold
                        break
                # At this point, we have enough capacity to handle the expected number of new sessions.
                # Now we make sure that we can't reduce anymore.
                # We mark the least occupied workers to lazy downscale, while keeping above threshold.
                capacity = 0
                for worker_id in self._workers[service_name]:
                    if not self._workers[service_name][worker_id].lazy_downscale:
                        capacity += (1 - self._workers[service_name][worker_id].utilisation) * N
                extra_capacity = capacity - expected_new_workers
                active_worker_ids = sorted(
                    filter(lambda worker_id: not self._workers[service_name][worker_id].lazy_downscale,
                           self._workers[service_name].keys()),
                    key=lambda worker_id: self._workers[service_name][worker_id].utilisation
                )
                for worker_id in active_worker_ids:
                    worker_extra_room = (1 - self._workers[service_name][worker_id].utilisation) * N
                    if worker_extra_room > extra_capacity:
                        self._workers[service_name][worker_id].lazy_downscale = True
                        logger.info(f"Worker {worker_id} marked for lazy downscale for {service_name}.")
                        extra_capacity -= worker_extra_room
                        continue
                    break

                # Now tear down any worker that is marked for lazy downscale, and is not utilised.
                for worker_id, worker in self._workers[service_name].items():
                    if not worker.lazy_downscale:
                        continue
                    if worker.utilisation > 0:
                        continue
                    # Try to shutdown. If it's not empty, it will give error.
                    await self._shutdown_worker(
                        service_name=service_name,
                        worker_id=worker_id
                    )
                # End

            finally:
                if service_exists:
                    if self._service_locks[service_name].locked():
                        self._service_locks[service_name].release()

    async def stake_claim(self, service_name: str, worker_id: str, session_id: str) -> str:
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
                self._ledger[service_name][session_id] = LedgerEntry(
                    worker_id=worker_id,
                    added_dt=current_utc()
                )
            return self._ledger[service_name][session_id].worker_id
            # End

        finally:
            if service_exists:
                if self._service_locks[service_name].locked():
                    self._service_locks[service_name].release()

    async def request_scale_up(self, service_name: str):
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
