import inspect
import logging
from datetime import timedelta
from functools import wraps
from typing import Dict, Type, Any, Callable, TypeVar, Protocol, Union

import ray

from stick_ray.abc import AbstractRoutedService, WorkerParams, ServiceParams
from stick_ray.controller import StickRayController
from stick_ray.routed_service_handle import SyncRoutedServiceHandle, RoutedServiceHandle
from stick_ray.stateful_worker import StatefulWorker

__all__ = [
    'RoutedService',
    'routed_service',
]

logger = logging.getLogger(__name__)


class RoutedService(AbstractRoutedService):
    """
    A class representing a routed service, which has a method for getting a handle to the service.
    All requests to the handle will be routed to the correct worker for this service. A common router is used for all
    routed services.
    """

    def __init__(self, service_name: str, service_params: ServiceParams | None = None):

        controller = StickRayController()

        if service_params is not None:
            # Register the service with the router
            controller.add_service(
                service_name=service_name,
                service_params=service_params
            )

        node_id = ray.get_runtime_context().get_node_id()
        router = controller.get_router(node_id=node_id)
        self._controller = controller
        self._router = router
        self._service_name = service_name

    def __new__(cls, *args, **kwargs):
        _deserialize = kwargs.pop('_deserialize', False)

        for key in ['controller', 'router', 'service_name']:
            if _deserialize and key not in kwargs:
                raise ValueError(f"Deserializing requires the argument '{key}'")

        # Check if deserializing
        if _deserialize:
            instance = super(RoutedService, cls).__new__(cls)
            instance._controller = kwargs.get('controller')
            instance._router = kwargs.get('router')
            instance._serice_name = kwargs.get('service_name')
            return instance
        else:
            return super(RoutedService, cls).__new__(cls)

    def __reduce__(self):
        # Return the class method for deserialization and the actor as an argument
        serialised_data = dict(
            controller=self._controller,
            router=self._router,
            service_name=self._service_name
        )
        return (self._deserialise, (serialised_data,))

    @classmethod
    def _deserialise(cls, kwargs):
        # Create a new instance, bypassing __init__ and setting the fields directly
        return cls.__new__(cls, _deserialize=True, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(router={self._router}, service_name={self._service_name})"

    def __hash__(self):
        # required for using in sets, etc.
        return hash(repr(self))

    def __eq__(self, other: AbstractRoutedService):
        # required for comparisons
        if isinstance(other, AbstractRoutedService):
            return repr(self) == repr(other)
        raise ValueError('Can only compare with `AbstractRoutedService`')

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
