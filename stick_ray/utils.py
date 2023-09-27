import asyncio
import hashlib
import inspect
import logging
import uuid
from datetime import datetime, tzinfo, timedelta
from typing import Any, Callable, Union, TypeVar, Coroutine

from stick_ray.abc import AbstractBackoffStrategy

logger = logging.getLogger(__name__)


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get a running async event loop if one exists, otherwise create one.

    This function serves as a proxy for the deprecating get_event_loop().
    It tries to get the running loop first, and if no running loop
    could be retrieved:
    - For python version <3.10: it falls back to the get_event_loop
        call.
    - For python version >= 3.10: it uses the same python implementation
        of _get_event_loop() at asyncio/events.py.

    Ideally, one should use high level APIs like asyncio.run() with python
    version >= 3.7, if not possible, one should create and manage the event
    loops explicitly.
    """
    import sys

    vers_info = sys.version_info
    if vers_info.major >= 3 and vers_info.minor >= 10:
        # This follows the implementation of the deprecating `get_event_loop`
        # in python3.10's asyncio. See python3.10/asyncio/events.py
        # _get_event_loop()
        try:
            loop = asyncio.get_running_loop()
            assert loop is not None
            return loop
        except RuntimeError as e:
            # No running loop, relying on the error message as for now to
            # differentiate runtime errors.
            if "no running event loop" in str(e):
                return asyncio.get_event_loop_policy().get_event_loop()
            else:
                raise e

    return asyncio.get_event_loop()


def deterministic_uuid(seed: str) -> uuid.UUID:
    """
    Generate a UUID using a deterministic hashing of a seed string.

    Args:
        seed: str, a string seed

    Returns:
        UUID

    """
    if not isinstance(seed, str):
        raise TypeError(f"Expected seed type `str`, got {type(seed)}.")
    m = hashlib.md5()
    m.update(seed.encode('utf-8'))
    new_uuid = uuid.UUID(m.hexdigest())
    return new_uuid


def set_datetime_timezone(dt: datetime, offset: Union[str, tzinfo]) -> datetime:
    """
    Replaces the datetime object's timezone with one from an offset.

    Args:
        dt: datetime, with out without a timezone set. If set, will be replaced.
        offset: tzinfo, or str offset like '-04:00' (which means EST)

    Returns:
        datetime with timezone set
    """
    if isinstance(offset, str):
        dt = dt.replace(tzinfo=None)
        return datetime.fromisoformat(f"{dt.isoformat()}{offset}")
    if isinstance(offset, tzinfo):
        return dt.replace(tzinfo=offset)
    raise ValueError(f"offset {offset} not understood.")


def current_utc() -> datetime:
    """
    Get the current datetime in UTC, with timezone set to UTC.

    Returns:
        datetime
    """
    return set_datetime_timezone(datetime.utcnow(), '+00:00')


def is_key_after_star(func: Callable, key: str):
    """
    Checks if a function has a keyword-only argument after a star argument.

    Args:
        func: Callable to check for keyword-only argument after star argument.
        key: keyword-only argument to check for.

    Returns:
        True if key is a keyword-only argument after star argument.
    """
    signature = inspect.signature(func)
    parameters = signature.parameters

    for name, param in parameters.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY and name == key:
            return True
    return False


async def loop_task(task_fn: Callable[[], Coroutine[Any, Any, None]], interval: timedelta | Callable[[], timedelta]):
    """
    Runs a task in a loop.

    Args:
        task_fn: the task
        interval: the interval between runs
    """
    while True:
        try:
            await task_fn()
        except Exception as e:
            logger.exception(f"Problem with task {task_fn.__name__}: {str(e)}")
        if callable(interval):
            await asyncio.sleep(interval().total_seconds())
            continue
        await asyncio.sleep(interval.total_seconds())


class IdentifyBackoffStrategy(AbstractBackoffStrategy):

    def __call__(self, attempt: int) -> timedelta:
        return timedelta(seconds=0)


class ValueMonitor:
    """
    Monitors a value by polling a coroutine at a given interval.
    """
    V = TypeVar('V')

    def __init__(self, coroutine_to_poll: Callable[[], Coroutine[Any, Any, V]],
                 callback: Callable[[V], None], init_poll_interval: timedelta,
                 error_backoff_strategy: AbstractBackoffStrategy | None = None):
        self.coroutine_to_poll = coroutine_to_poll
        self.callback = callback
        self.poll_interval = init_poll_interval
        self.error_backoff_strategy = error_backoff_strategy or IdentifyBackoffStrategy()  # No backoff by default
        self.last_value = None
        self.monitor_task = None
        self.error_count = 0

    async def start_monitoring(self):
        while True:
            try:
                new_value = await self.coroutine_to_poll()

                if new_value != self.last_value:
                    self.last_value = new_value
                    self.callback(new_value)

                self.error_count = 0  # Reset error count on success
                await asyncio.sleep(self.poll_interval.total_seconds())

            except asyncio.CancelledError:
                logging.info("Monitoring task was cancelled!")
                break
            except Exception as e:
                logging.error(f"Error while monitoring value: {e}")
                self.error_count += 1
                backoff_time = self.error_backoff_strategy(self.error_count)
                await asyncio.sleep(backoff_time.total_seconds())

    async def start(self):
        """
        Start monitoring the value.
        """
        if self.monitor_task is None:
            self.monitor_task = asyncio.create_task(self.start_monitoring())

    async def stop(self):
        """
        Stop monitoring the value.
        """
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            self.monitor_task = None
