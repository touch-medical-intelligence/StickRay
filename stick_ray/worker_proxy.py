import asyncio
import atexit
import logging
from datetime import timedelta, datetime
from typing import Type, Tuple, Any, Dict, Callable, Coroutine, Set

import ray
from ray import ObjectRef
from ray.actor import exit_actor, ActorHandle
from ray.util.metrics import Histogram

from stick_ray.controller import WorkerParams
from stick_ray.eventbus import EventBus
from stick_ray.namespace import NAMESPACE
from stick_ray.stateful_worker import StatefulWorker
from stick_ray.utils import get_or_create_event_loop, loop_task, current_utc, is_key_after_star, SerialisableBaseModel

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL: timedelta = timedelta(seconds=3)


class _Dummy:
    pass


class SessionAlreadyExists(Exception):
    """
    Exception raised when a session id is not found.
    """
    pass


class SessionItem(SerialisableBaseModel):
    lock: asyncio.Lock  # lock for session
    last_use_dt: datetime  # last time a session was used


class WorkerUpdate(SerialisableBaseModel):
    utilisation: float  # the capacity of the worker, 0 means empty. 1 means full.
    lazy_downscale: bool  # whether the worker is marked for downscaling soon
    session_ids: Set[str]  # session ids managed by the worker


class SessionNotFound(Exception):
    pass


class BackPressure(Exception):
    pass


class BaseWorkerProxy:
    def __init__(self, actor: ActorHandle):
        self._actor = actor

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return BaseWorkerProxy(**kwargs)

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        serialised_data = dict(
            actor=self._actor,
        )
        return self.__class__._deserialise, (serialised_data,)


class WorkerProxy(BaseWorkerProxy):
    """
    Proxy for a stateful worker. This class is responsible for managing sessions, and ferrying requests to the correct
    session. It also provides a control loop to manage the worker, and a health check method.
    """

    def __init__(self, service_name: str, worker_id: str, worker_params: WorkerParams):
        actor_name = self._worker_actor_name(service_name=service_name, worker_id=worker_id)

        try:
            actor = ray.get_actor(actor_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {actor_name}")
        except ValueError:
            worker_actor_options = worker_params.worker_actor_options.copy()
            worker_actor_options['name'] = actor_name
            worker_actor_options['namespace'] = NAMESPACE
            # No restart, another worker will be started cleanly by controller
            worker_actor_options['max_restart'] = 0
            worker_actor_options['max_task_retries'] = -1
            if worker_actor_options.pop('lifetime', None):  # make sure we don't accidentally make a detached one
                logger.warning(f"Bad pattern: do not set `lifetime=detached` for {service_name}.")

            dynamic_cls = self._dynamic_worker_actor_cls(service_name=service_name)
            actor = ray.remote(dynamic_cls).options(**worker_actor_options).remote(
                service_name=service_name,
                worker_id=worker_id,
                **worker_params.worker_kwargs
            )
            ray.get(actor.health_check.remote())
            logger.info(f"Created new {actor_name}")
        super().__init__(actor=actor)

    @staticmethod
    def _dynamic_worker_actor_cls(service_name: str) -> Type:
        """
        Create a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.

        Args:
            service_name: the name of the service

        Returns:
            a dynamic class
        """
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"StickRayReplica:{service_name}",
            (_WorkerProxy,),
            dict(_WorkerProxy.__dict__),
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

    async def health_check(self):
        """
        A no-op health check.
        """
        return await self._actor.health_check.remote()

    async def mark_lazy_downscale(self):
        """
        Marks the worker for downscaling soon. Cannot add more sessions during this time.
        """
        await self._actor.mark_lazy_downscale.remote()

    def unmark_lazy_downscale(self):
        """
        Marks the worker to stay alive, in case it was marked for lazy downscale.
        """
        await self._actor.unmark_lazy_downscale.remote()

    async def create_session(self, session_id: str):
        """
        Creates a session for the given session id.

        Args:
            session_id: session id, e.g. a user UUID.

        Raises:
            SessionAlreadyExists if session already exists
            RejectSession if worker is hot, or marked for lazy downscale
        """
        return await self._actor.create_session.remote(session_id=session_id)

    async def close_session(self, session_id: str):
        """
        Closes a session on the worker.

        Args:
            session_id: session id

        Raises:
            SessionNotFound if session not found.
        """
        return await self._actor.close_session.remote(session_id=session_id)

    async def start(self):
        """
        Starts the worker.
        """
        return await self._actor.start.remote()

    async def shutdown(self):
        """
        Shuts down the worker.
        """
        return await self._actor.shutdown.remote()

    async def check_session(self, session_id: str) -> bool:
        """
        Checks if the given session id is managed by the worker.

        Args:
            session_id: session id to check

        Returns:
            true if managed
        """
        return await self._actor.check_session.remote(session_id=session_id)

    async def ferry(self, method: str, data_ref_tuple: Tuple[ObjectRef], session_id: str,
                    grant: bool) -> Any:
        """
        Ferries a method to this worker and then returns the result as an object ref.

        Args:
            method: method name to ferry
            data_ref_tuple: an object ref of tuple (args, kwargs)
            session_id: session id to ferry to
            grant: whether session creation granted

        Returns:
            an object ref to the result of the operation, i.e. the task is awaited, and then put into object store.

        Raises:
            SessionNotFound if granted=False, and if session_id not currently managed, i.e. expired session or it never existed.
            BackPressure  if granted=False, and if worker rejects placement
            AttributeError if method is not found.
            SyntaxError if method is not defined correctly.
            ValueError if session_id is found in the kwargs, as this results in an overwrite.
        """
        return await self._actor.ferry.remote(method=method, data_ref_tuple=data_ref_tuple, session_id=session_id)


class _WorkerProxy:
    """
    Proxy for a stateful worker. This class is responsible for managing sessions, and ferrying methods to the correct
    session. It also provides a control loop to manage the worker, and a health check method.
    """

    def __init__(self, service_name: str, worker_id: str, worker_params: WorkerParams):
        self._service_name = service_name
        self._worker_id = worker_id
        self._worker_params = worker_params
        if not issubclass(worker_params.worker_cls, StatefulWorker):
            raise TypeError(
                f"worker_cls must be a subclass of StatefulWorker, but got {worker_params.worker_cls.__name__}"
            )
        self._worker = worker_params.worker_cls(**worker_params.worker_kwargs)
        self._lazy_downscale: bool = False

        self._worker_lock = asyncio.Lock()
        self._sessions_lock = asyncio.Lock()
        self._session_items: Dict[str, SessionItem] = dict()

        self._event_bus = EventBus()
        self._method_func_cache: Dict[str, Callable[..., Coroutine]] = dict()

        self.latency_ms = Histogram(
            'latency_ms',
            description="Measures how long calls take in ms.",
            boundaries=list(map(lambda i: 2 ** i, range(15))),  # up to 16384 ms
            tag_keys=('service_name', 'method')
        )
        self.latency_ms.set_default_tags({"service_name": service_name})
        # TODO(Joshuaalbert): add a metric for the utilisation of worker.

        # Start control loop on actor creation, and assign clean up atexit
        self._control_long_running_task: asyncio.Task | None = get_or_create_event_loop().create_task(
            self._initialise()
        )

        atexit.register(self._atexit_clean_up())

    def _atexit_clean_up(self):
        async def stop_worker():
            # Any open sessions will *not* be closed.
            async with self._worker_lock:
                async with self._sessions_lock:
                    try:
                        await self._worker._shutdown()
                        logger.info(f"Successful shut down of worker.")
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.error(f"Problem cleaning up. {str(e)}")
                        raise

        # Run the worker shutdown
        loop = get_or_create_event_loop()
        try:
            loop.run_until_complete(stop_worker())
        except asyncio.CancelledError:
            logger.error("Worker shutdown as cancelled before finishing.")
        except Exception as e:
            logger.error(f"Problem stopping worker. {str(e)}")
            raise
        finally:
            loop.close()

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
        async with self._worker_lock:
            async with self._sessions_lock:
                try:
                    await self._worker._initialise()
                    # worker-specific startup protocol
                    await self._worker._start()
                    logger.info(f"Successful start up.")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Problem in worker start: {str(e)}")
                    raise e
        await self._run_control_loop()

    async def _run_control_loop(self):
        logger.info(f"Starting worker control loop for {self.__class__.__name__}!")
        while True:
            try:
                await asyncio.gather(
                    loop_task(task_fn=self._gossip, interval=HEARTBEAT_INTERVAL),
                    loop_task(task_fn=self._prune_expired_sessions, interval=timedelta(seconds=1)),
                    return_exceptions=False
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def _utilisation(self) -> float:
        # TODO: implement backpressure based on resource usage
        return len(self._session_items) / self._worker_params.max_concurrent_sessions

    async def _gossip(self):
        # Heatbeat/Backpressure/Session map signal
        update = WorkerUpdate(
            utilisation=await self._utilisation(),
            lazy_downscale=self._lazy_downscale,
            session_ids=set(self._session_items.keys())
        )
        await self._event_bus.write(
            key=f"{self._service_name}_{self._worker_id}",
            item=update
        )

    async def _prune_expired_sessions(self):
        # Should not prune a session while it is being ferried, so we use the same locking structure.
        for session_id in list(self._session_items):
            session_exists = False
            try:
                await self._sessions_lock.acquire()
                try:
                    if session_id not in self._session_items:
                        continue
                    session_exists = True
                    await self._session_items[session_id].lock.acquire()
                finally:
                    self._sessions_lock.release()

                # Start prune
                if current_utc() > self._session_items[session_id].last_use_dt + self._worker_params.expiry_period:
                    logger.info(f"Session {session_id} expired. Closing down.")
                    try:
                        await self._worker._close_session(session_id=session_id)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.info(f"Failed to close session {session_id}: {str(e)}")
                        raise
                    finally:
                        del self._session_items[session_id]
                # End prune
            finally:
                if session_exists:
                    if self._session_items[session_id].lock.locked():
                        self._session_items[session_id].lock.release()

    def health_check(self):
        """
        A no-op health check.
        """
        return

    def mark_lazy_downscale(self):
        """
        Marks the worker for downscaling soon. Cannot add more sessions during this time.
        """
        self._lazy_downscale = True

    def unmark_lazy_downscale(self):
        """
        Marks the worker to stay alive, in case it was marked for lazy downscale.
        """
        self._lazy_downscale = False

    async def _granted_create_session(self, session_id: str):
        """
        Creates a session for the given session id.

        Args:
            session_id: session id, e.g. a user UUID.
        """
        try:
            await self._worker._create_session(session_id=session_id)
            self._session_items[session_id] = SessionItem(
                lock=asyncio.Lock(),
                last_use_dt=current_utc()
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Failed to create session {session_id}: {str(e)}")
            if session_id in self._session_items:
                del self._session_items[session_id]
            raise e

    async def close_session(self, session_id: str):
        """
        Closes a session on the worker.

        Args:
            session_id: session id

        Raises:
            SessionNotFound if session not found.
        """

        session_exists = False
        try:
            await self._sessions_lock.acquire()
            try:
                # Check if service is registered.
                if session_id not in self._session_items:
                    raise SessionNotFound(session_id)
                session_exists = True
                await self._session_items[session_id].lock.acquire()
            finally:
                self._sessions_lock.release()

            # Start operation
            try:
                await self._worker._close_session(session_id=session_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Failed to close session {session_id}: {str(e)}")
                raise e
            finally:
                del self._session_items[session_id]
            # End operation

        finally:
            if session_exists:
                if self._session_items[session_id].lock.locked():
                    self._session_items[session_id].lock.release()

    async def shutdown(self):
        """
        Gracefully shuts down the worker.
        """
        # The atexit handler will do all the work
        exit_actor()

    def check_session(self, session_id: str) -> bool:
        """
        Checks if the given session id is managed by the worker.

        Args:
            session_id: session id to check

        Returns:
            true if managed
        """
        return session_id in self._session_items

    async def ferry(self, method: str, data_ref_tuple: Tuple[ObjectRef], session_id: str,
                    grant: bool) -> Tuple[ObjectRef]:
        """
        Ferries a method to this worker and then returns the result as an object ref.

        Args:
            method: method name to ferry
            data_ref_tuple: an object ref of tuple (args, kwargs)
            session_id: session id to ferry to
            grant: whether this ferry grants permission to create session if needed.

        Returns:
            an object ref to the result of the operation, i.e. the task is awaited, and then put into object store.

        Raises:
            SessionNotFound if session_id not currently managed, i.e. expired session or it never existed.
            BackPressure if worker rejects placement
            AttributeError if method is not found.
            SyntaxError if method is not defined correctly.
            ValueError if session_id is found in the kwargs, as this results in an overwrite.
        """

        # Mesure latency from the top
        start_dt = current_utc()

        # Idempotent, so no need to lock further.
        if method in self._method_func_cache:  # Lookup method if used before
            func = self._method_func_cache[method]
        else:  # Assess correctness of method, and cache
            available = sorted(
                filter(lambda x: not x.startswith('_'), set(dir(self._worker)) - set(dir(_Dummy())))
            )
            if method not in available:
                raise AttributeError(
                    f"Invalid method {method}. Available methods are {available}."
                )
            # Get method
            func = getattr(self._worker, method)
            # Ensure function spec is good.
            if not is_key_after_star(func, 'session_id'):
                raise SyntaxError(
                    f"Method definition must have session_id as keyword-only arg, "
                    f"i.e. `def {method}(..., *, session_id)`"
                )
            self._method_func_cache[method] = func

        # The reason we use this pattern is so that we can release the main lock as soon as the
        # session lock is acquired. In order for this to be robust all other modifications to the sessions must
        # follow the same pattern.

        session_exists = False
        try:
            await self._sessions_lock.acquire()
            try:
                # Check if session exists
                if session_id in self._session_items:
                    pass
                else:
                    if grant:
                        await self._granted_create_session(session_id=session_id)
                    if self._lazy_downscale or (await self._utilisation()):
                        raise BackPressure()
                    raise SessionNotFound(session_id)

                session_exists = True
                await self._session_items[session_id].lock.acquire()
            finally:
                self._sessions_lock.release()

            # Start ferry
            # Get the inputs locally.
            (data_ref,) = data_ref_tuple
            data = await data_ref  # retrieve the input locally
            args = data['args']
            kwargs = data['kwargs']
            # Set session id in kwargs
            if 'session_id' in kwargs:
                raise ValueError(f'You have a key session_id in your kwargs, which is reserved for session id.')
            kwargs['session_id'] = session_id
            result = await func(*args, **kwargs)
            dt = current_utc() - start_dt
            self.latency_ms.observe(dt.total_seconds() * 1e3, tags=dict(method=method))
            logger.info(f"Handled {method} in session {session_id} in {dt.total_seconds() * 1e3:0.1f} ms.")
            result_ref = ray.put(result)
            return (result_ref,)
            # End ferry

        finally:
            if session_exists:
                if self._session_items[session_id].lock.locked():
                    self._session_items[session_id].lock.release()
