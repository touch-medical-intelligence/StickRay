import asyncio
import atexit
import logging
from datetime import timedelta
from typing import Any, Dict, Callable, Coroutine, Tuple, Set

from fair_async_rlock import FairAsyncRLock
from ray import ObjectRef
from ray.util.metrics import Histogram

from stick_ray.eventbus import EventBus
from stick_ray.utils import SerialisableBaseModel, get_or_create_event_loop, current_utc, is_key_after_star

__all__ = [
    'StatefulWorker',
    'StatefulSessionNotFound'
]
logger = logging.getLogger(__name__)
HEARTBEAT_INTERVAL: timedelta = timedelta(seconds=3)


class StatefulSessionNotFound(Exception):
    """
    Exception raised when a session id is not found.
    """
    pass


class _dummy: pass


class SessionItem(SerialisableBaseModel):
    lock: FairAsyncRLock
    state: Any


class StatefulWorker:
    """
    Represents a stateful worker. Handles sessions for sticky connections, and sends heart beats to router.
    """

    def __init__(self, worker_id: str, **kwargs):
        self._lock = asyncio.Lock()
        self._session_items: Dict[str, SessionItem] = dict()

        self._stop_event = asyncio.Event()
        self._event_bus = EventBus(name='routed_services')
        self._worker_id = worker_id
        self._method_func_cache: Dict[str, Callable[..., Coroutine]] = dict()

        self.latency_ms = Histogram(
            'latency_ms',
            description="Measures how long calls take in ms.",
            boundaries=list(map(lambda i: 2 ** i, range(15))),  # up to 16384 ms
            tag_keys=('routed_service_name', 'method')
        )
        self.latency_ms.set_default_tags({"routed_service_name": self.__class__.__name__})

        loop = get_or_create_event_loop()
        task = loop.create_task(self._run_control_loop())

        def clean_up(task, loop):
            task.cancel()
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))

        atexit.register(clean_up, task, loop)

    async def _run_control_loop(self):
        logger.info(f"Starting worker control loop for {self.__class__.__name__}!")
        while not self._stop_event.is_set():
            # Run all individual loops
            try:
                await asyncio.gather(
                    self._send_heartbeat(),
                    self._update(),
                    return_exceptions=False
                )
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def _send_heartbeat(self):
        while not self._stop_event.is_set():
            # Heatbeat/Backpressure signal
            await self._event_bus.write(key=f"{self._worker_id}_backpressure", item=await self._too_busy())
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _update(self):
        last_update = set()
        while not self._stop_event.is_set():
            # Update sessions managed
            update = set(self._session_items.keys())
            if not (update == last_update):
                await self._event_bus.write(key=f"{self._worker_id}_update", item=update)
                last_update = update
            await asyncio.sleep(HEARTBEAT_INTERVAL.total_seconds())

    async def _too_busy(self) -> bool:
        return False

    async def _stop(self):
        self._stop_event.set()

    async def ferry(self, method: str, data_ref_tuple: Tuple[ObjectRef], session_id: str) -> Any:
        """
        Ferries a method to this worker, and returns as a task, i.e. an awaitable result.

        Args:
            method: method name to ferry
            data_ref_tuple: an object ref of tuple (args, kwargs)
            session_id: session id to ferry to

        Returns:
            the result of the operation, i.e. the task is awaited.

        Raises:
            StatefulSessionNotFound if session_id not currently managed, i.e. expired session or it never existed.
            ValueError if session_id is found in the kwargs, as this results in an overwrite.
        """
        start_dt = current_utc()
        async with self._lock:
            if method in self._method_func_cache:  # Lookup method if used before
                func = self._method_func_cache[method]
            else:  # Assess correctness of method, and cache
                # We allow close_session from outside
                ignore_methods = {'ferry', 'get_session_state', 'set_session_state', 'create_session', 'start',
                                  'shutdown', 'check_session', 'get_session_ids', 'health_check'}
                available = sorted(
                    filter(lambda x: not x.startswith('_'), set(dir(self)) - set(dir(_dummy())) - ignore_methods)
                )
                if method.startswith('_'):
                    raise AttributeError(
                        f"Invalid method {method}. Available methods are {available}."
                    )
                # Get method
                func = getattr(self, method, None)
                if func is None:
                    raise AttributeError(
                        f"Invalid method {method}. Available methods are {available}."
                    )
                # Double ensure function spec is good.
                if not is_key_after_star(func, 'session_id'):
                    raise SyntaxError(
                        f"Method definition must have session_id as keyword-only arg, e.g. `def {method}(..., *, session_id)`")
                self._method_func_cache[method] = func
            # Ensure session id is valid
            if session_id not in self._session_items:  # may have been pruned before getting lock
                raise StatefulSessionNotFound(session_id)
            session_lock = self._session_items[session_id].lock
            acquired_event = asyncio.Event()

            async def eval_task():
                async with session_lock:  # we block awaiting task until this lock is acquired.
                    acquired_event.set()
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
                    finish_dt = current_utc()
                    dt = finish_dt - start_dt
                    logger.info(f"Handled {method} in session {session_id} in {dt.total_seconds() * 1e3:0.1f} ms.")
                    self.latency_ms.observe(dt.total_seconds() * 1e3, tags=dict(method=method))
                    return result

            # create the task for work to be done inside lock so that session id membership is locked until started.
            task = asyncio.create_task(eval_task())
            await asyncio.sleep(0)
            # Wait until acquired before releasing worker lock
            await acquired_event.wait()
        return await task

    async def get_session_state(self, session_id: str) -> Any:
        """
        Get the session state for a session id.

        Args:
            session_id: session id to get state for

        Returns:
            the session state

        Raises:
            AttributeError if it's not found.
        """
        if session_id not in self._session_items:
            raise StatefulSessionNotFound(session_id)
        return self._session_items[session_id].state

    async def set_session_state(self, session_id: str, session_state: Any):
        """
        Set the session state for the session id.

        Args:
            session_id: session id
            session_state: a state object.
        """
        if session_id not in self._session_items:
            raise StatefulSessionNotFound(session_id)
        self._session_items[session_id].state = session_state

    async def _close_session(self, session_id: str):
        raise NotImplementedError()

    async def _create_session(self, session_id: str):
        raise NotImplementedError()

    async def _start(self):
        raise NotImplementedError()

    async def _shutdown(self):
        raise NotImplementedError()

    async def close_session(self, *, session_id: str):
        """
        Closes the session for a given session id.

        Args:
            session_id: sessoin id to close down.

        """
        async with self._lock:
            try:
                await self._close_session(session_id=session_id)
            finally:
                if session_id in self._session_items:  # it could be missing
                    del self._session_items[session_id]

    async def create_session(self, session_id: str) -> bool:
        """
        Creates a session for the given session id.

        Args:
            session_id: session id, e.g. a user UUID.

        Returns:
            True iff session what successfully created, False
        """
        async with self._lock:
            if session_id in self._session_items:
                raise RuntimeError(f"Session {session_id} already exists.")
            self._session_items[session_id] = SessionItem(lock=FairAsyncRLock(), state=None)
            # TODO: Add backpressure here with rejection if too busy
            try:
                await self._create_session(session_id=session_id)
                if session_id not in self._session_items:
                    raise RuntimeError(f"Session {session_id} disappeared.")
                return True
            except Exception as e:
                logger.error(f"Failed to create session {session_id}: {str(e)}")
                if session_id in self._session_items:
                    del self._session_items[session_id]
                return False

    async def start(self):
        """
        Starts the worker.
        """
        async with self._lock:
            await self._start()
            logger.info(f"Successful start up.")

    async def shutdown(self):
        """
        Shuts down the worker.
        """
        async with self._lock:
            try:
                await self._shutdown()
            finally:
                await self._stop()
                logger.info(f"Successful shut down.")

    async def check_session(self, session_id: str) -> bool:
        """
        Checks if the given session id is managed by the worker.

        Args:
            session_id: session id to check

        Returns:
            true if managed
        """
        async with self._lock:
            return session_id in self._session_items

    async def get_session_ids(self) -> Set[str]:
        """
        Gets all current managed session ids.

        Returns:
            a set of session ids.
        """
        async with self._lock:
            return set(self._session_items.keys())

    async def health_check(self):
        """
        Simply returns, acting as a health check.
        """
        return
