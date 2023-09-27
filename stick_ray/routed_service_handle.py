import asyncio
import logging
from time import sleep

import ray
from ray import ObjectRef

from stick_ray.abc import AbstractRouter, AbstractRoutedServiceHandle, SyncFerryProtocol, FerryProtocol
from stick_ray.common import RetryRouting, RoutingFailedError

__all__ = [
    'RoutedServiceHandle',
    'SyncRoutedServiceHandle'
]

logger = logging.getLogger(__name__)


class BaseRoutedServiceHandle(AbstractRoutedServiceHandle):
    """
    A class representing a handle that can be used to transparently dish out requests to a routed service.
    """

    def __init__(self, router: AbstractRouter, service_name: str):
        """
        Initialises the handle.

        Args:
            router: router for the service
            service_name: name of the service
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
