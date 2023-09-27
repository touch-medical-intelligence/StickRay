import asyncio
import atexit
import logging
from datetime import timedelta
from typing import Dict, Tuple, TypeVar, Type

import ray
from pydantic import confloat
from ray import ObjectRef
from ray.actor import exit_actor
from ray.util.metrics import Gauge
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.abc import AbstractRouter, AbstractStickRayController, AbstractWorkerProxy
from stick_ray.actor_interface import ActorInterface
from stick_ray.common import BackPressure, WorkerUpdate, SessionNotFound, RetryRouting, ServiceNotFoundError, \
    WorkerNotFoundError, SerialisableBaseModel
from stick_ray.consistent_hash_ring import ConsistentHashRing, NoAvailableNode, EmptyRing
from stick_ray.eventbus import EventBus
from stick_ray.namespace import NAMESPACE
from stick_ray.utils import get_or_create_event_loop, loop_task
from stick_ray.worker_proxy import WorkerProxy

__all__ = [
    'Router'
]

logger = logging.getLogger(__name__)

V = TypeVar('V')


class Router(AbstractRouter, ActorInterface):
    """
    A class representing a router. This is never invoked directly.
    """

    def __init__(self, node_id: str, controller: AbstractStickRayController):
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
                # "max_task_retries": -1, # Don't set if max_restarts==0
                # Schedule the controller on the given node.
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node_id, soft=True),
                "namespace": NAMESPACE,
                "max_concurrency": 15000  # Needs to be large, as there should be no limit.
            }
            dynamic_cls = self.dynamic_router_cls()
            actor_kwargs = dict(controller=controller)
            actor = ray.remote(dynamic_cls).options(**router_actor_options).remote(**actor_kwargs)
            ray.get(actor.health_check.remote())
            logger.info(f"Created new {router_name}")

        ActorInterface.__init__(self, actor=actor)

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


class WorkerItem(SerialisableBaseModel):
    worker: AbstractWorkerProxy
    utilisation: confloat(ge=0.) = 0.
    listen_task: asyncio.Task | None = None


class ServiceItem(SerialisableBaseModel):
    worker_entries: Dict[str, WorkerItem]  # worker_id -> WorkerItem
    session_map: Dict[str, str]  # session_id -> worker_id
    ring: ConsistentHashRing  # ring
    active: asyncio.Event  # active event
    lock: asyncio.Lock  # service lock
    listen_to_gossip_interval: timedelta = timedelta(seconds=10)  # gossip interval


class _Router(AbstractRouter):
    """
    A class representing a routed service. This is never invoked directly.
    """
    _update_metrics_interval = timedelta(seconds=60)
    _log_interval = timedelta(seconds=60)

    def __init__(self, controller: AbstractStickRayController):

        self._router_lock = asyncio.Lock()  # for any manipulation of service registry, very little contention expected.
        self._services_lock = asyncio.Lock()  # Lock for changes the set of services
        self._workers_lock = asyncio.Lock()  # Lock for changing the set of workers per service
        self._service_items: Dict[str, ServiceItem] = dict()  # service_name -> ServiceItem

        self.active_sessions = Gauge(
            'active_sessions',
            description="Measures how many sessions are live currently across all workers for a service.",
            tag_keys=('service_name',)
        )

        self._controller = controller
        self._event_bus = EventBus()

        # Start control loop on actor creation, and assign clean up atexit
        self._control_long_running_task: asyncio.Task | None = get_or_create_event_loop().create_task(
            self._initialise()
        )

        atexit.register(self._atexit_clean_up)

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
                # loop.close()
                ...

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
                    loop_task(task_fn=self._log_info, interval=self._log_interval),
                    loop_task(task_fn=self._update_metrics, interval=self._update_metrics_interval),
                    return_exceptions=False
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def shutdown(self):
        # Triggers atexit handlers
        exit_actor()

    async def pause_service(self, service_name: str):
        if service_name not in self._service_items:
            raise ServiceNotFoundError(f"Service {service_name} not found.")
        self._service_items[service_name].active.clear()

    async def unpause_service(self, service_name: str):
        if service_name not in self._service_items:
            raise ServiceNotFoundError(f"Service {service_name} not found.")
        self._service_items[service_name].active.set()

    async def health_check(self):
        """
        Announce health check.
        """
        return

    def _listen_task(self, service_name: str, worker_id: str) -> asyncio.Task:
        """
        Create a task that listens to gossip from a worker.

        Args:
            service_name: the service name
            worker_id: the worker id

        Returns:
            task

        Raises:
            ServiceNotFoundError: if service is not registered.
            WorkerNotFoundError: if worker is not registered.
        """

        async def listen_task():
            # Discard if service or worker removed since.
            if service_name not in self._service_items:
                return

            await self._service_items[service_name].active.wait()

            # Discard if service or worker removed since.
            if (service_name not in self._service_items) or (
                    worker_id not in self._service_items[service_name].worker_entries):
                return

            # No-wait peek
            try:
                worker_update: WorkerUpdate = await self._event_bus.peek(
                    key=f"{service_name}_{worker_id}",
                    timeout=0
                )
            except asyncio.TimeoutError:
                return

            # Discard if service or worker removed since.
            if (service_name not in self._service_items) or (
                    worker_id not in self._service_items[service_name].worker_entries):
                return

            # Update backpressure
            self._service_items[service_name].worker_entries[worker_id].utilisation = worker_update.utilisation
            # Update session map
            for session_id in worker_update.session_ids:
                self._service_items[service_name].session_map[session_id] = worker_id

        # Create task
        task = asyncio.create_task(
            loop_task(task_fn=listen_task,
                      interval=lambda: self._service_items[service_name].listen_to_gossip_interval)
        )
        return task

    async def _delete_worker(self, service_name: str, worker_id: str):
        if (service_name not in self._service_items) or (
                worker_id not in self._service_items[service_name].worker_entries):
            # Double check because yield of control allows for races
            return
        # for sessions that point to this worker, we remove those too
        for session_id in list(self._service_items[service_name].session_map):
            if self._service_items[service_name].session_map[session_id] == worker_id:
                del self._service_items[service_name].session_map[session_id]
        # Remove from hashring
        self._service_items[service_name].ring.remove_node(node=worker_id)
        # Close listen task
        self._service_items[service_name].worker_entries[worker_id].listen_task.cancel()
        try:
            await self._service_items[service_name].worker_entries[worker_id].listen_task
        except asyncio.CancelledError:
            logger.info(f"Cancelled listen task for {service_name} {worker_id}.")
        finally:
            del self._service_items[service_name].worker_entries[worker_id]

    async def _add_worker(self, service_name: str, worker_id: str, worker_proxy: AbstractWorkerProxy):
        if (service_name not in self._service_items) or (
                worker_id not in self._service_items[service_name].worker_entries):
            # Double check because yield of control allows for races
            return
        self._service_items[service_name].worker_entries[worker_id] = WorkerItem(
            worker=worker_proxy,
            utilisation=0.,
            listen_task=self._listen_task(service_name=service_name, worker_id=worker_id)
        )
        self._service_items[service_name].ring.add_node(node=worker_id, weight=1)

    async def set_services(self, services: Dict[str, Dict[str, WorkerProxy]]):
        for service_name in services:
            if service_name in self._service_items:
                logger.warning(f"Service {service_name} already registered.")
            else:
                self._service_items[service_name] = ServiceItem(
                    worker_entries=dict(),
                    session_map=dict(),
                    ring=ConsistentHashRing(),
                    active=asyncio.Event(),
                    lock=asyncio.Lock()
                )
            # Remove workers that are no longer active
            for worker_id in list(self._service_items[service_name].worker_entries):
                if worker_id not in services[service_name]:
                    await self._delete_worker(
                        service_name=service_name,
                        worker_id=worker_id
                    )

            # Add new workers
            for worker_id, worker in services[service_name].items():
                if worker_id in self._service_items[service_name].worker_entries:
                    await self._add_worker(service_name=service_name, worker_id=worker_id, worker_proxy=worker)

    async def delete_service(self, service_name: str):
        """
        Deletes a service.

        Args:
            service_name: the name of the service
        """
        if service_name not in self._service_items:
            logger.warning(f"Service {service_name} not registered.")
            return
        try:
            for worker_id in list(self._service_items[service_name].worker_entries):
                await self._delete_worker(service_name=service_name, worker_id=worker_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Problem deleting service {service_name}: {str(e)}")
            raise
        finally:
            del self._service_items[service_name]

    async def _log_info(self):
        """
        Logs information about the current routing table.
        """

        def count_sessions_per_worker(service_name: str) -> Dict[str, int]:
            sessions_per_worker: Dict[str, int] = dict()
            for session_id in self._service_items[service_name].session_map:
                worker_id = self._service_items[service_name].session_map[session_id]
                if worker_id not in sessions_per_worker:
                    sessions_per_worker[worker_id] = 0
                sessions_per_worker[worker_id] += 1
            return sessions_per_worker

        def get_info(service_name: str) -> str:
            """
            Get information string of the current routing table.
            """
            info = f"Service: {service_name}\n"
            info += f"# Sessions aware of: {len(self._service_items[service_name].session_map):04d}\n"
            for worker_id, session_count in count_sessions_per_worker(service_name=service_name):
                utilisation = self._service_items[service_name].worker_entries[worker_id].utilisation
                info += f"\t({utilisation * 100:0.2f}%) Worker {worker_id}: {session_count} sessions\n"
            return info

        for service_name in self._service_items:
            if service_name not in self._service_items:
                continue
            logger.info(get_info(service_name=service_name))

    async def _update_metrics(self):
        """
        Updates metrics, such as the number of active sessions per service.
        """
        # Get the number of sessions per service
        for service_name in self._service_items:
            tags = {"service_name": service_name}
            active_sessions = len(self._service_items[service_name].session_map)
            self.active_sessions.set(active_sessions, tags=tags)

    async def set_gossip_interval(self, service_name: str, interval: timedelta):
        if service_name not in self._service_items:
            raise ServiceNotFoundError(f"Service {service_name} not found.")
        self._service_items[service_name].listen_to_gossip_interval = interval

    async def ferry(self, service_name: str, method: str, session_id: str, data_ref_tuple: Tuple[ObjectRef]) \
            -> Tuple[ObjectRef]:
        # The reason we use this pattern is so that we can release the main lock as soon as the
        # service lock is acquired. In order for this to be robust all other modifications to the services must
        # follow the same pattern.
        service_exists = False
        try:
            await self._services_lock.acquire()
            try:
                # Check if service is registered.
                if service_name not in self._service_items:
                    raise ServiceNotFoundError(service_name)
                service_exists = True
                # Block until service is unpaused.
                await self._service_items[service_name].lock.acquire()
            finally:
                self._services_lock.release()

            # Start ferry

            # Wait until service is unpaused, as workers are being updated.
            await self._service_items[service_name].active.wait()
            try:
                for worker_id in self._service_items[service_name].ring.node_iter(session_id):
                    worker_entry = self._service_items[service_name].worker_entries[worker_id]
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
                        if granted_worker_id not in self._service_items[service_name].worker_entries:
                            raise WorkerNotFoundError(
                                f"Worker {granted_worker_id} not found."
                            )
                        worker_entry = self._service_items[service_name].worker_entries[granted_worker_id]
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
                if self._service_items[service_name].lock.locked():
                    self._service_items[service_name].lock.release()
