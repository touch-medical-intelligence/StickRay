import asyncio
import atexit
import logging
from datetime import timedelta, datetime
from typing import Type, Tuple, Dict, Callable, Coroutine, TypeVar, Any

import ray
from ray import ObjectRef
from ray.actor import exit_actor
from ray.util.metrics import Histogram

from stick_ray.abc import AbstractWorkerProxy, WorkerParams
from stick_ray.actor_interface import ActorInterface
from stick_ray.common import BackPressure, WorkerUpdate, SessionNotFound, WorkerStillBusyError, SerialisableBaseModel
from stick_ray.eventbus import EventBus
from stick_ray.namespace import NAMESPACE
from stick_ray.stateful_worker import StatefulWorker
from stick_ray.utils import get_or_create_event_loop, loop_task, current_utc, is_key_after_star

logger = logging.getLogger(__name__)


class _Dummy:
    pass


class SessionItem(SerialisableBaseModel):
    lock: asyncio.Lock  # lock for session
    last_use_dt: datetime  # last time a session was used
    expiry_task: asyncio.Task | None = None  # task to expire session


class WorkerProxy(AbstractWorkerProxy, ActorInterface):
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
            worker_actor_options.pop('max_task_retries', None)  # Don't set if max_restart==0
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
        ActorInterface.__init__(self, actor=actor)

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


class _WorkerProxy(AbstractWorkerProxy):
    """
    Proxy for a stateful worker. This class is responsible for managing sessions, and ferrying methods to the correct
    session. It also provides a control loop to manage the worker, and a health check method.
    """

    _prune_interval = timedelta(seconds=10)

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

        self._worker_lock = asyncio.Lock()  # For locking changes to the worker
        self._session_items_lock = asyncio.Lock()  # For locking changes to the session items
        self._session_items: Dict[str, SessionItem] = dict()  # session_id -> SessionItem

        self._gossip_interval = timedelta(seconds=10)
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

        atexit.register(self._atexit_clean_up)

    def _atexit_clean_up(self):
        async def stop_worker():
            async with self._worker_lock:
                # TODO: shutdown sessions, perhaps?
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
            logger.error("Worker shutdown as cancelled before finishing shutting down.")
        except Exception as e:
            logger.error(f"Problem stopping worker. {str(e)}")
            raise
        finally:
            # Ensure to shutdown the control loop
            if self._control_long_running_task is not None:
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
        async with self._worker_lock:
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
                    loop_task(task_fn=self._gossip, interval=lambda: self._gossip_interval),
                    return_exceptions=False
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(str(e))
                logger.info("Restarting control loop")

    async def _utilisation(self) -> float:
        """
        Get the utilisation of the worker.

        Returns:
            utilisation of the worker, between 0 and 1.
        """
        # TODO: implement based on resource usage
        return len(self._session_items) / self._worker_params.max_concurrent_sessions

    async def _gossip(self):
        """
        Gossip to the event bus.
        """
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

    async def health_check(self):
        """
        A no-op health check.
        """
        return

    async def set_gossip_interval(self, interval: timedelta):
        self._gossip_interval = interval

    async def mark_lazy_downscale(self):
        self._lazy_downscale = True

    async def unmark_lazy_downscale(self):
        self._lazy_downscale = False

    async def _set_expiry(self, session_id: str):
        """
        Sets the expiry task for the given session id.

        Args:
            session_id: session id to set expiry for
        """
        if session_id not in self._session_items:
            return

        expiry_task = self._session_items[session_id].expiry_task

        # Cancel existing task if it exists
        if (expiry_task is not None) and (not expiry_task.done()):
            expiry_task.cancel()
            try:
                await expiry_task
            except asyncio.CancelledError:
                pass

        async def _wait_expire():
            # Waits then expires session, this will be cancelled if session is used again.
            await asyncio.sleep(self._worker_params.expiry_period.total_seconds())
            # Importantly, we grab the session lock if it's still here.
            if session_id not in self._session_items:
                return
            session_lock = self._session_items[session_id].lock
            async with session_lock:
                try:
                    await self._worker._close_session(session_id=session_id)
                finally:
                    del self._session_items[session_id]

        self._session_items[session_id].expiry_task = get_or_create_event_loop().create_task(_wait_expire())

    async def _granted_create_session(self, session_id: str, session_lock: asyncio.Lock | None = None):
        """
        Creates a session for the given session id. Idempotent.

        Args:
            session_id: session id, e.g. a user UUID.
            lock: lock to use for session, if None, will create a new lock.

        Raises:
            ValueError: if sessions are not locked when calling this method.
        """

        if not self._session_items_lock.locked():
            raise ValueError(f"Sessions lock must be locked to create a session.")

        # This is idempotent so that concurrent race conditions are resolved at the session creation level.
        if session_id in self._session_items:
            return

        # Set expiry needs session to exist, so we need to create it, but get the lock here first.
        self._session_items[session_id] = SessionItem(
            lock=session_lock or asyncio.Lock(),
            last_use_dt=current_utc()
        )
        success = False
        try:
            await self._worker._create_session(session_id=session_id)
            await self._set_expiry(session_id=session_id)
            success = True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Failed to create session {session_id}: {str(e)}")
            raise e
        finally:
            if not success:
                # So we don't end up with sessions upon failure.
                del self._session_items[session_id]

    async def shutdown(self):
        if len(self._session_items) > 0:
            raise WorkerStillBusyError(f"Still handling {len(self._session_items)} sessions.")
        # The atexit handler will do all the work
        exit_actor()

    async def check_session(self, session_id: str) -> bool:
        return session_id in self._session_items

    async def ferry(self, method: str, data_ref_tuple: Tuple[ObjectRef], session_id: str,
                    grant: bool) -> Tuple[ObjectRef]:

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

        async def ferry_op() -> Tuple[ObjectRef]:
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
            await self._set_expiry(session_id=session_id)
            return (result_ref,)

        try:
            return await self._safe_session_fn(session_id=session_id, fn=ferry_op, grant=grant)
        except SessionNotFound:
            if self._lazy_downscale or (await self._utilisation() > 1.):
                raise BackPressure()
            raise

    T = TypeVar('T')

    async def _safe_session_fn(self, session_id: str, fn: Callable[..., Coroutine[Any, Any, T]], grant: bool) -> T:
        """
        Safely runs a function that requires session lock, with optional grant of session creation

        Args:
            session_id: session id to run function on
            fn: function to run
            grant: whether to grant session creation if session does not exist

        Returns:
            the result of the function

        Raises:
            SessionNotFound: if session_id not currently managed and `grant` is False.
        """

        # First, try to get the lock for the session. If it doesn't exist, we'll get a KeyError.
        if session_id not in self._session_items:
            if grant:
                # We need the global session items lock, because granting yields control
                async with self._session_items_lock:
                    # This is idempotent, so no worries about races.
                    await self._granted_create_session(session_id=session_id)
                    session_lock = self._session_items[session_id].lock
                    await session_lock.acquire()
            else:
                raise SessionNotFound(session_id)
        else:
            # Since we do not yield control at any point, we can safely find the session.
            session_lock = self._session_items[session_id].lock
            # When we yield someone could remove `session_id` from `session_items`, so we need to check again below.
            await session_lock.acquire()

        try:
            # This is the double-check which is necessary to handle race conditions.
            if session_id not in self._session_items:
                if grant:
                    async with self._session_items_lock:
                        # We pass in the currently held session lock to set.
                        await self._granted_create_session(session_id=session_id, session_lock=session_lock)
                else:
                    raise SessionNotFound(session_id)

            # perform operations on the session
            try:
                return await fn()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Problem in session {session_id}: {str(e)}")
                raise e

        finally:
            session_lock.release()
