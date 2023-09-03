import asyncio
import atexit
import inspect
import logging
from datetime import datetime, timedelta
from functools import wraps
from time import monotonic_ns
from typing import Dict, Set, Type, Any, Callable, TypeVar, Tuple, Protocol, Union

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.util.metrics import Gauge
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.eventbus import EventBus
from stick_ray.namespace import NAMESPACE
from stick_ray.stateful_worker import HEARTBEAT_INTERVAL, StatefulWorker
from stick_ray.utils import SerialisableBaseModel, get_or_create_event_loop, deterministic_uuid, current_utc

__all__ = [
    'RoutedServiceHandle',
    'routed_service',
]

logger = logging.getLogger(__name__)


class AddressNotFound(Exception):
    def __init__(self, address: Union[str, None] = None):
        super().__init__(f"Address {address} not found.")


class WorkerEntry(SerialisableBaseModel):
    worker_actor: ActorHandle
    last_add_dt: datetime
    backpressure: bool


class AddresBookEntry(SerialisableBaseModel):
    worker_id: str
    created_dt: datetime
    last_query_dt: datetime


class Router:
    """
    A class representing a routed service. This is never invoked directly.
    """

    def __init__(self, worker_cls: Type[StatefulWorker],
                 worker_actor_options: Dict[str, Any],
                 worker_kwargs: Dict[str, Any],
                 max_concurrent_sessions: int,
                 expiry_period: timedelta,
                 min_num_workers: int):
        """
        Initialises a router.

        Args:
            worker_cls: the worker class
            worker_actor_options: the worker actor options (see ray.remote)
            worker_kwargs: the worker kwargs to pass in a worker creation
            max_concurrent_sessions: maximum number of concurrent sessions per worker, before spinning up a new worker
            expiry_period: how long a session last since last interaction before being closed down
            min_num_workers: the minimum number of persistent workers. The number is maintained, but not the specific
                workers. I.e. the first worker may not be the one that lasts forever, if min_num_workers=1.
        """

        self.lock = asyncio.Lock()  # for any manipulation of address book, or workers this must be gotten.
        self.workers: Dict[str, WorkerEntry] = dict()
        self.address_book: Dict[str, AddresBookEntry] = dict()
        self.max_concurrent_sessions = max_concurrent_sessions
        self.expiry_period = expiry_period
        self.min_num_workers = min_num_workers
        self.worker_cls = worker_cls
        self.worker_actor_options = worker_actor_options
        self.worker_kwargs = worker_kwargs

        self.active_sessions = Gauge(
            'active_sessions',
            description="Measures how many sessions are live currently across all workers.",
            tag_keys=('routed_service_name',)
        )
        self.active_sessions.set_default_tags({"routed_service_name": worker_cls.__name__})

        self._event_bus = EventBus(name='routed_services')

        loop = get_or_create_event_loop()

        task = loop.create_task(self._run_control_loop())

        def clean_up(task, loop):
            task.cancel()
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))

        atexit.register(clean_up, task, loop)

    async def health_check(self):
        """
        Announce health check.
        """
        logger.info(f"{self.__class__.__name__} is up!")
        return

    def _count_worker_sessions(self, worker_id):
        return sum(map(lambda w: 1, filter(lambda item: item.worker_id == worker_id, self.address_book.values())))

    async def _run_control_loop(self):
        logger.info(f"Starting router control loop for {self.worker_cls.__name__}!")
        while True:
            # Run all individual loops
            try:
                await asyncio.gather(
                    self._check_worker_health(),
                    self._reconcile_address_book(),
                    self._prune_expired_sessions(),
                    self._shutdown_empty_workers(),
                    self._log_info(),
                    self._update_metrics(),
                    return_exceptions=False
                )
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def _log_info(self):
        def info() -> str:
            """
            Get information string of the current routing table.
            """
            info = f"# Workers: {len(self.workers):04d}\t# Sessions: {len(self.address_book):04d}"
            for worker_id in self.workers:
                count = self._count_worker_sessions(worker_id=worker_id)
                if self.workers[worker_id].backpressure:
                    info += f"\n* Worker {worker_id}: {count} sessions"
                else:
                    info += f"\nWorker {worker_id}: {count} sessions"
            return info

        log_interval = timedelta(minutes=1)
        t0 = current_utc()
        while True:
            async with self.lock:
                if (current_utc() > t0 + log_interval) and (len(self.address_book) > 0):
                    logger.info(info())
                    t0 = current_utc()
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _check_worker_health(self):
        while True:
            if len(self.workers) == 0:
                await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())
                continue
            for worker_id in list(self.workers.keys()):
                await asyncio.sleep(0)
                non_responsive = True
                t0 = current_utc()
                while current_utc() < t0 + timedelta(minutes=1):  # TODO: could make + 5 sigma of latency stats
                    try:
                        backpressure: bool = await self._event_bus.peek(
                            key=f"{worker_id}_backpressure",
                            timeout=HEARTBEAT_INTERVAL.total_seconds()
                        )
                        non_responsive = False
                        if backpressure:  # do something with backpressure
                            if not isinstance(backpressure, bool):
                                raise TypeError(f"Expected bool backpressure, got {type(backpressure)}")
                            async with self.lock:
                                if worker_id not in self.workers:  # Check in case it disappeared before getting lock, weird
                                    continue
                                self.workers[worker_id].backpressure = backpressure
                        break
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.exception(f"Problem with event bus: {str(e)}")
                        continue

                if non_responsive:  # unhealthy
                    async with self.lock:
                        logger.warning(f"Worker {worker_id} of {self.worker_cls.__name__} is not healthy!")
                        count = 0
                        for session_id in list(self.address_book):
                            if self.address_book[session_id].worker_id == worker_id:
                                del self.address_book[session_id]
                                logger.info(f"Removed {session_id} from {worker_id}.")
                                count += 1
                        logger.info(f"Removed {count} sessions for {worker_id}.")
                        if worker_id in self.workers:  # Check in case it disappeared before getting lock, weird
                            del self.workers[worker_id]
                            logger.info(f"Removed worker {worker_id}")
                        continue

    async def _reconcile_address_book(self):
        while True:
            if len(self.workers) == 0:
                await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())
                continue
            for worker_id in list(self.workers.keys()):
                await asyncio.sleep(0)
                try:
                    update: Set[str] = await self._event_bus.peek(
                        key=f"{worker_id}_update",
                        timeout=HEARTBEAT_INTERVAL.total_seconds() * 2
                    )
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.exception(f"Problem getting update from worker {worker_id}:{str(e)}")
                    continue
                # Update address book
                async with self.lock:
                    if worker_id not in self.workers:  # pruned before getting lock
                        continue
                    for session_id in update:  # Ensure consistency
                        if session_id not in self.address_book:  # not found locally, add new entry
                            self.address_book[session_id] = AddresBookEntry(
                                worker_id=worker_id,
                                created_dt=current_utc(),
                                last_query_dt=current_utc() - timedelta(days=10000)
                            )
                            continue
                        if self.address_book[session_id].worker_id != worker_id:  # found, but inconsistent, update
                            logger.info(
                                f"Moving session {session_id} from {self.address_book[session_id].worker_id} to {worker_id}"
                            )
                            try:  # Close other other one if possible
                                await self.workers[
                                    self.address_book[session_id].worker_id].worker_actor.close_session.remote(
                                    session_id=session_id
                                )
                            except Exception as e:
                                logger.exception(e)
                            finally:
                                self.address_book[session_id] = AddresBookEntry(
                                    worker_id=worker_id,
                                    created_dt=current_utc(),
                                    last_query_dt=current_utc() - timedelta(days=10000)
                                )
                            continue

    async def _prune_expired_sessions(self):
        # Should not be able to prune while ferrying, so we need to acquire the lock.
        while True:
            if len(self.address_book) == 0:
                await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())
                continue
            for session_id in list(self.address_book.keys()):
                await asyncio.sleep(0)
                async with self.lock:
                    if session_id not in self.address_book:  # pruned before getting lock
                        continue
                    if current_utc() > self.address_book[session_id].last_query_dt + self.expiry_period:  # expired
                        logger.info(f"Session {session_id} expired. Closing down.")
                        try:
                            worker_entry = self.workers[self.address_book[session_id].worker_id]
                            await worker_entry.worker_actor.close_session.remote(session_id=session_id)
                        except Exception as e:
                            logger.exception(str(e))
                        finally:
                            del self.address_book[session_id]
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _shutdown_empty_workers(self):
        while True:
            if len(self.workers) == 0:
                await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())
                continue
            for worker_id in list(self.workers.keys()):
                await asyncio.sleep(0)
                async with self.lock:
                    if worker_id not in self.workers:  # shutdown before getting lock
                        continue
                    if len(self.workers) > self.min_num_workers:
                        count = self._count_worker_sessions(worker_id=worker_id)
                        if count == 0:
                            logger.info(f"Worker {worker_id} empty. Closing down.")
                            try:
                                await self.workers[worker_id].worker_actor.shutdown.remote()
                            except Exception as e:
                                logger.exception(str(e))
                            finally:
                                # Note, any existing references will prevent shutdown of actor until released.
                                del self.workers[worker_id]
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _update_metrics(self):
        while True:
            # Adjust max_concurrent
            self.active_sessions.set(len(self.address_book))
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _create_new_worker(self) -> str:
        # Make sure calling method has lock
        worker_id = str(deterministic_uuid(str(monotonic_ns())))
        worker_name = f"{self.worker_cls.__name__}.worker.{worker_id[:6]}"
        worker_actor_options = self.worker_actor_options.copy()
        worker_actor_options['name'] = worker_name
        worker_actor_options.pop('lifetime', None)  # make sure we don't accidentally make a detached one
        new_worker = ray.remote(self.worker_cls).options(**worker_actor_options).remote(
            worker_id=worker_id,
            **self.worker_kwargs
        )
        await new_worker.start.remote()
        logger.info(f"Started {worker_name}")
        self.workers[worker_id] = WorkerEntry(
            worker_actor=new_worker,
            last_add_dt=current_utc() - timedelta(days=10000),
            backpressure=False
        )

        return worker_id

    async def _get_assigned_worker(self, session_id: str) -> WorkerEntry:
        async with self.lock:
            # Get assigned worker id, creating worker if necessary.
            if session_id in self.address_book:
                assigned_worker_id = self.address_book[session_id].worker_id
                self.address_book[session_id].last_query_dt = current_utc()
            else:
                # get LRU worker, or new worker if all full
                assigned_worker_id: Union[str, None] = None
                sorted_workers = reversed(
                    sorted(self.workers, key=lambda worker_id: self.workers[worker_id].last_add_dt)
                )
                for worker_id in sorted_workers:
                    count = self._count_worker_sessions(worker_id=worker_id)
                    if (count < self.max_concurrent_sessions) and not self.workers[worker_id].backpressure:
                        assigned_worker_id = worker_id
                        break
                if assigned_worker_id is None:  # make worker
                    assigned_worker_id = await self._create_new_worker()
                self.workers[assigned_worker_id].last_add_dt = current_utc()
                await self.workers[assigned_worker_id].worker_actor.create_session.remote(session_id=session_id)
                self.address_book[session_id] = AddresBookEntry(worker_id=assigned_worker_id,
                                                                created_dt=current_utc(),
                                                                last_query_dt=current_utc())
            return self.workers[assigned_worker_id]

    async def ferry(self, method: str, session_id: str, data_ref_tuple: Tuple[ObjectRef]) -> Tuple[ObjectRef]:
        """
        Forwards a request to the correct worker with the current session. If no session can be found, creates a new one.

        Args:
            method: the name of the method to be ferried. Only methods not starting with '_' are permitted.
            session_id: the id of the session. No constraints on the shape of this string. Typically, a UUID.
            data_ref_tuple: a tuple of an object ref containing the args to the method. These are resolved on the other
                side, so that the ferry itself never touches the object.

        Returns:
            a tuple of object ref representing the results.
        """
        worker_entry = await self._get_assigned_worker(session_id=session_id)

        obj_ref = worker_entry.worker_actor.ferry.remote(
            method=method,
            data_ref_tuple=data_ref_tuple,
            session_id=session_id
        )
        return (obj_ref,)


class RoutedServiceHandle:
    """
    A class representing a handle that can be used to transparently dish out requests to a routed service.
    """

    def __init__(self, router: ActorHandle, sync: bool):
        """
        Initialised routed service handle.

        Args:
            router: an actor handle for the router
            sync: whether to produce a sync handle, or async handle.
        """
        self._router = router
        self._sync = sync

    def __getattr__(self, item):
        if item.startswith('_'):
            raise AttributeError(f"Only public methods are reachable. {item} invalid public method name.")

        if self._sync:
            def ferry(*args, **kwargs) -> ObjectRef:
                session_id = kwargs.pop('session_id', None)
                data = dict(
                    args=args,
                    kwargs=kwargs
                )
                data_ref_tuple = (ray.put(data),)
                if session_id is None:
                    raise ValueError(f"Missing session_id")
                (obj_ref,) = ray.get(self._router.ferry.remote(method=item,
                                                               session_id=session_id,
                                                               data_ref_tuple=data_ref_tuple))
                return obj_ref
        else:
            async def ferry(*args, **kwargs) -> ObjectRef:
                session_id = kwargs.pop('session_id', None)
                data = dict(
                    args=args,
                    kwargs=kwargs
                )
                data_ref_tuple = (ray.put(data),)
                if session_id is None:
                    raise ValueError(f"Missing session_id")
                (obj_ref,) = await self._router.ferry.remote(method=item,
                                                             session_id=session_id,
                                                             data_ref_tuple=data_ref_tuple)
                return obj_ref
        return ferry


class NoRoutedServiceFound(Exception):
    pass


class BaseRoutedService:
    def __init__(self, router: ActorHandle):
        self._router = router

    def __hash__(self):
        # required for using in sets, etc.
        return hash(repr(self._router))

    def __eq__(self, other: 'BaseRoutedService'):
        # required for comparisons
        if not isinstance(other, BaseRoutedService):
            raise ValueError('Can only compare with `BaseRoutedService`')
        return repr(self._router) == repr(other._router)

    @staticmethod
    def dynamic_cls(name: str) -> Type:
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"Router:{name}",
            (Router,),
            dict(Router.__dict__),
        )

    @staticmethod
    def router_name(name: str) -> str:
        router_name = f"{name}.router"
        return router_name

    def get_handle(self, sync: bool = True) -> RoutedServiceHandle:
        """
        Gets a handle for the routed service.

        Args:
            sync: whether it will be a sync or async handle.

        Returns:
            a RoutedServiceHandle

        Examples:
            # sync handles are used like
            handle = service.get_handle()
            ray.get(handle.some_func(..., session_id='abc'))

            # an async handle are used like
            handle = service.get_handle(sync=False)
            await (await handle.some_func(..., session_id='abc'))
            # the first await creates a task (so the work starts being done).
            # the second await gets the result
        """
        return RoutedServiceHandle(router=self._router, sync=sync)


class FetchedRoutedService(BaseRoutedService):
    def __init__(self, name: str):

        router_name = self.router_name(name)
        try:
            router = ray.get_actor(router_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {router_name}")
        except ValueError:
            raise NoRoutedServiceFound(f"Tried to fetch {router_name}, but it was not found.")

        super().__init__(router=router)


class RoutedService(BaseRoutedService):
    def __init__(self, name: str,
                 worker_cls: Type[StatefulWorker],
                 worker_actor_options: Union[Dict[str, Any], None],
                 worker_kwargs: Union[Dict[str, Any], None],
                 max_concurrent_sessions: int,
                 expiry_period: timedelta,
                 min_num_workers: int):
        if max_concurrent_sessions < 1:
            raise ValueError(f"max_concurrent_sessions must be >= 1, got {max_concurrent_sessions}")
        if expiry_period.total_seconds() < 0:
            raise ValueError(f"expiry_period must be strictly positive, got {expiry_period}")
        worker_actor_options = worker_actor_options or dict()
        worker_kwargs = worker_kwargs or dict()

        head_node_id = ray.get_runtime_context().get_node_id()
        router_name = self.router_name(name)

        try:
            router = ray.get_actor(router_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {router_name}")
        except ValueError:
            router_actor_options = {
                "num_cpus": 0,
                "name": router_name,
                "lifetime": "detached",
                "max_restarts": -1,
                "max_task_retries": -1,
                # Schedule the controller on the head node with a soft constraint. This
                # prefers it to run on the head node in most cases, but allows it to be
                # restarted on other nodes in an HA cluster.
                "scheduling_strategy": NodeAffinitySchedulingStrategy(head_node_id, soft=True),
                "namespace": NAMESPACE,
                "max_concurrency": 15000  # Needs to be large, as there should be no limit.
            }

            router_kwargs = dict(
                worker_cls=worker_cls,
                worker_actor_options=worker_actor_options,
                worker_kwargs=worker_kwargs,
                max_concurrent_sessions=max_concurrent_sessions,
                expiry_period=expiry_period,
                min_num_workers=min_num_workers
            )

            dynamic_cls = self.dynamic_cls(name)

            router = ray.remote(dynamic_cls).options(**router_actor_options).remote(**router_kwargs)
            ray.get(router.health_check.remote())
            logger.info(f"Created new {router_name}")
        super().__init__(router=router)


V = TypeVar('V')


class FProtocol(Protocol):
    __call__: Callable[..., RoutedService]  # The main function signature, change None to the actual return type of f
    fetch: Callable[[], FetchedRoutedService]  # The g method signature, change None to the actual return type of g


def routed_service(
        expiry_period: timedelta,
        name: Union[str, None] = None,
        worker_actor_options: Union[Dict[str, Any], None] = None,
        max_concurrent_sessions: int = 10,
        min_num_workers: int = 0
) -> Callable[[Type[V]], FProtocol]:
    def decorator(worker_cls: Type[V]) -> FProtocol:
        if not issubclass(worker_cls, StatefulWorker):
            raise ValueError(f"Only StatefulWorker subclasses can be made into routed services. Got {worker_cls}.")

        if name is None:
            _name = worker_cls.__name__
        else:
            _name = name

        @wraps(worker_cls)
        def wrapped(**kwargs):
            return RoutedService(name=_name,
                                 worker_cls=worker_cls,
                                 worker_actor_options=worker_actor_options,
                                 worker_kwargs=kwargs,
                                 max_concurrent_sessions=max_concurrent_sessions,
                                 expiry_period=expiry_period,
                                 min_num_workers=min_num_workers
                                 )

        def fetch() -> FetchedRoutedService:
            return FetchedRoutedService(name=_name)

        wrapped.fetch = fetch
        wrapped.__signature__ = inspect.signature(worker_cls)
        return wrapped

    return decorator
