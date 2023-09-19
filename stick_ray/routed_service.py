import asyncio
import inspect
import logging
from abc import abstractmethod, ABC
from datetime import timedelta
from functools import wraps
from time import sleep
from typing import Dict, Type, Any, Callable, TypeVar, Protocol, Union

import ray
from ray import ObjectRef

from stick_ray.controller import ServiceParams, StickRayController, WorkerParams
from stick_ray.router import Router, RetryRouting
from stick_ray.stateful_worker import StatefulWorker

__all__ = [
    'RoutedServiceHandle',
    'SyncRoutedServiceHandle',
    'RoutedService',
    'routed_service',
]

logger = logging.getLogger(__name__)


class RoutingFailedError(Exception):
    pass


class SyncFerryProtocol(Protocol):
    """
    A protocol for ferrying
    """

    def __call__(self, *args, **kwargs) -> ObjectRef:
        """
        Returns the output of ferried method.

        Args:
            *args: args to method.
            **kwargs: kwargs to method, MUST contain session_id

        Returns:
            return output of ferried method.

        Raises:
            RoutingFailedError if the routing failed.
        """
        ...


class FerryProtocol(Protocol):
    """
    A protocol for ferrying
    """

    async def __call__(self, *args, **kwargs) -> ObjectRef:
        """
        Returns the output of ferried method.

        Args:
            *args: args to method.
            **kwargs: kwargs to method, MUST contain session_id

        Returns:
            return output of ferried method.

        Raises:
            RoutingFailedError if the routing failed.
        """
        ...


class BaseRoutedServiceHandle(ABC):
    """
    A class representing a handle that can be used to transparently dish out requests to a routed service.
    """

    def __init__(self, router: Router, service_name: str):
        """
        Initialised routed service handle.

        Args:
            router: an actor handle for the router
            sync: whether to produce a sync handle, or async handle.
        """
        self._router = router
        self._service_name = service_name

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        serialised_data = dict(
            router=self._router,
            service_name=self._service_name,
        )
        return self.__class__._deserialise, (serialised_data,)

    @abstractmethod
    def __getattr__(self, item) -> FerryProtocol | SyncFerryProtocol:
        """
        Returns a method that ferries the input to the correct location.

        Args:
            item: method name

        Returns:
            A ferry function
        """
        ...


class SyncRoutedServiceHandle(BaseRoutedServiceHandle):
    """
    A class representing a handle that can be used to transparently dish out requests to a routed service.
    """

    def __getattr__(self, item) -> SyncFerryProtocol:
        # TODO(JoshuaAlbert): Consider exposing a Protocol like ray.remote, so it's more natural for ray users.
        def ferry(*args, **kwargs) -> ObjectRef:
            session_id = kwargs.pop('session_id', None)
            if session_id is None:
                raise ValueError(f"Missing session_id")
            data = dict(
                args=args,
                kwargs=kwargs
            )
            data_ref_tuple = (ray.put(data),)
            retry_wait = 1
            while True:
                try:
                    (obj_ref,) = ray.get(self._router.ferry(
                        service_name=self._service_name,
                        method=item,
                        session_id=session_id,
                        data_ref_tuple=data_ref_tuple
                    ))
                    return obj_ref
                except RetryRouting:
                    logger.warning(
                        f"Service {self._service_name} over capacity. Will retry routing in {retry_wait} seconds")
                    sleep(retry_wait)
                    retry_wait *= 2
                    if retry_wait > 64:
                        raise RoutingFailedError(f"Service {self._service_name} routing failed.")
                    continue

        return ferry


class RoutedServiceHandle(BaseRoutedServiceHandle):
    """
    A class representing a handle that can be used to transparently dish out requests to a routed service.
    """

    def __getattr__(self, item) -> FerryProtocol:
        # TODO(JoshuaAlbert): Consider exposing a Protocol for ray.remote, so it's more natural for ray users.
        async def ferry(*args, **kwargs) -> ObjectRef:
            session_id = kwargs.pop('session_id', None)
            if session_id is None:
                raise ValueError(f"Missing session_id")
            data = dict(
                args=args,
                kwargs=kwargs
            )
            data_ref_tuple = (ray.put(data),)
            retry_wait = 1
            while True:
                try:
                    (obj_ref,) = await self._router.ferry(
                        service_name=self._service_name,
                        method=item,
                        session_id=session_id,
                        data_ref_tuple=data_ref_tuple
                    )
                    return obj_ref
                except RetryRouting:
                    logger.warning(
                        f"Service {self._service_name} over capacity. Will retry routing in {retry_wait} seconds")
                    await asyncio.sleep(retry_wait)
                    retry_wait *= 2
                    if retry_wait > 64:
                        raise RoutingFailedError(f"Service {self._service_name} routing failed.")
                    continue

        return ferry


class AbstractRoutedService(ABC):
    """
    A class representing a routed service.
    """

    @abstractmethod
    def __hash__(self):
        ...

    @abstractmethod
    def __eq__(self, other: 'AbstractRoutedService'):
        ...

    @abstractmethod
    def get_handle(self, sync: bool = True) -> RoutedServiceHandle | SyncRoutedServiceHandle:
        """
        Gets a handle for the routed service.

        Args:
            sync: whether it will be a sync or async handle.

        Returns:
            a RoutedServiceHandle or SyncRoutedServiceHandle
        """
        ...


class BaseRoutedService(AbstractRoutedService):
    """
    A class representing a routed service, which has a method for getting a handle to the service.
    All requests to the handle will be routed to the correct worker for this service. A common router is used for all
    routed services.
    """

    def __init__(self, controller: StickRayController, router: Router, service_name: str):
        self._controller = controller
        self._router = router
        self._service_name = service_name

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return BaseRoutedService(**kwargs)  # This obfuscates the original subclass of routed service, but it's fine.

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        serialised_data = dict(
            controller=self._controller,
            router=self._router,
            service_name=self._service_name
        )
        return self.__class__._deserialise, (serialised_data,)

    def __repr__(self):
        return f"{self.__class__.__name__}(router={self._router}, service_name={self._service_name})"

    def __hash__(self):
        # required for using in sets, etc.
        return hash(repr(self))

    def __eq__(self, other: AbstractRoutedService):
        # required for comparisons
        if isinstance(other, BaseRoutedService):
            return repr(self) == repr(other)
        raise ValueError('Can only compare with `BaseRoutedService`')

    def get_handle(self, sync: bool = True) -> RoutedServiceHandle | SyncRoutedServiceHandle:
        if sync:
            return SyncRoutedServiceHandle(
                router=self._router,
                service_name=self._service_name,
            )
        else:
            return RoutedServiceHandle(
                router=self._router,
                service_name=self._service_name,
            )


class RoutedService(BaseRoutedService):
    """
    A class representing a routed service. This is never invoked directly.
    """

    def __init__(self, service_name: str, service_params: ServiceParams | None = None):
        controller = StickRayController()

        if service_params is not None:
            # Register the service with the router
            controller.register_service(
                service_name=service_name,
                service_params=service_params
            )

        node_id = ray.get_runtime_context().get_node_id()
        router = controller.get_router(node_id=node_id)

        super().__init__(controller=controller, router=router, service_name=service_name)


V = TypeVar('V')


class RoutedServiceWrapperProtocol(Protocol):
    """
    A protocol for a routed service.
    """

    def __call__(self, **kwargs) -> RoutedService:
        ...

    def fetch(self) -> RoutedService:
        """
        Fetches a routed service, without deploying a new service.

        Returns:
            a RoutedService
        """
        ...

    def get_handle(self, sync: bool = True) -> RoutedServiceHandle:
        """
        Gets a handle for the routed service.

        Args:
            sync: whether it will be a sync or async handle.

        Returns:
            a RoutedServiceHandle
        """
        ...

    # TODO(Joshuaalbert): Add stop service here.


def routed_service(
        expiry_period: timedelta,
        name: Union[str, None] = None,
        worker_actor_options: Union[Dict[str, Any], None] = None,
        max_concurrent_sessions: int = 10,
        min_num_workers: int = 0
) -> Callable[[Type[V]], RoutedServiceWrapperProtocol]:
    """
    A decorator that turns a StatefulWorker into a routed service.

    Args:
        expiry_period: how long to keep a session alive since last interaction
        name: the name of the service. If None, the name of the class will be used.
        worker_actor_options: the worker actor options (see ray.remote)
        max_concurrent_sessions: maximum number of concurrent sessions per worker, before spinning up a new worker
        min_num_workers: the minimum number of persistent workers. The number is maintained, but not the specific
            workers. I.e. the first worker may not be the one that lasts forever, if min_num_workers=1.

    Returns:
        a decorator that can be applied to a StatefulWorker to turn it into a routed service.
    """

    def decorator(worker_cls: Type[V]) -> RoutedServiceWrapperProtocol:
        if not issubclass(worker_cls, StatefulWorker):
            raise ValueError(f"Only StatefulWorker subclasses can be made into routed services. Got {worker_cls}.")

        if name is None:
            _name = worker_cls.__name__
        else:
            _name = name

        @wraps(worker_cls)
        def wrapped(**kwargs):
            """
            Deploys a new routed service.
            """
            worker_params = WorkerParams(
                worker_cls=worker_cls,
                worker_actor_options=worker_actor_options or dict(),
                worker_kwargs=kwargs,
                max_concurrent_sessions=max_concurrent_sessions,
                expiry_period=expiry_period
            )
            service_params = ServiceParams(
                worker_params=worker_params,
                min_num_workers=min_num_workers
            )
            return RoutedService(
                service_name=_name,
                service_params=service_params
            )

        def fetch() -> RoutedService:
            """
            Fetches a routed service, without deploying a new service.
            """
            # Fetch without deploying a new service
            return RoutedService(service_name=_name)

        def get_handle(sync: bool = True) -> RoutedServiceHandle:
            """
            Gets a handle for the routed service.

            Args:
                sync: whether it will be a sync or async handle.

            Returns:
                a RoutedServiceHandle
            """
            service = fetch()
            return service.get_handle(sync=sync)

        wrapped.fetch = fetch
        wrapped.get_handle = get_handle
        wrapped.__signature__ = inspect.signature(worker_cls)
        return wrapped

    return decorator
