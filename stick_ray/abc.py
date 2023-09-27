from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Dict, Any, Tuple, Union, List, Type, Protocol

from pydantic import conint, validator
from ray import ObjectRef

from stick_ray.common import SerialisableBaseModel


class AbstractActorInterface(ABC):
    """
    An interface to actors, that:
    1. preserves annotations and type checking.
    2. automatically handles efficient serialisation and deserialisation.
    """

    ...


class AbstractStatefulWorker(ABC):
    """
    Base class for stateful worker.
    """

    @abstractmethod
    async def _create_session(self, session_id: str):
        """
        Called when a session is created. This is where you would initialise the session state, and store it.

        Args:
            session_id: session id to create
        """
        ...

    @abstractmethod
    async def _close_session(self, session_id: str):
        """
        Called when a session is closed. This is where you would clean up the session state, and remove it.

        Args:
            session_id: session id to close
        """
        ...

    @abstractmethod
    async def _start(self):
        """
        Called when the worker is started. This can be used to create any resources needed for the worker, e.g.
        database connections.
        """
        ...

    @abstractmethod
    async def _shutdown(self):
        """
        Called when the worker is shut down. This can be used to clean up any resources needed for the worker, e.g.
        database connections.
        """
        ...


class AbstractWorkerProxy(ABC):
    """
    Proxy for a stateful worker. This class is responsible for managing sessions, and ferrying requests to the correct
    session. It also provides a control loop to manage the worker, and a health check method.
    """

    @abstractmethod
    async def health_check(self):
        """
        A no-op health check.
        """
        ...

    @abstractmethod
    async def mark_lazy_downscale(self):
        """
        Marks the worker for downscaling soon. Cannot add more sessions during this time.
        """
        ...

    @abstractmethod
    async def unmark_lazy_downscale(self):
        """
        Marks the worker to stay alive, in case it was marked for lazy downscale.
        """
        ...

    @abstractmethod
    async def set_gossip_interval(self, interval: timedelta):
        """
        Sets the gossip interval for the worker.

        Args:
            interval: gossip interval
        """
        ...

    @abstractmethod
    async def shutdown(self):
        """
        Shuts down the worker.

        Raises:
            WorkerStillBusyError: if worker is still busy with sessions when shutdown is called.
        """
        ...

    @abstractmethod
    async def check_session(self, session_id: str) -> bool:
        """
        Checks if the given session id is managed by the worker.

        Args:
            session_id: session id to check

        Returns:
            true if managed
        """
        ...

    @abstractmethod
    async def ferry(self, method: str, data_ref_tuple: Tuple[ObjectRef], session_id: str, grant: bool) -> Any:
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
            SessionNotFound: if granted=False, and if session_id not currently managed, i.e. expired session or it never existed.
            BackPressure:  if granted=False, and if worker rejects placement
            AttributeError: if method is not found.
            SyntaxError: if method is not defined correctly.
            ValueError: if session_id is found in the kwargs, as this results in an overwrite.
        """
        ...


class AbstractRouter(ABC):
    """
    A class representing a router. This is never invoked directly.
    """

    @abstractmethod
    async def shutdown(self):
        """
        Gracefully shutdown router.
        """
        ...

    @abstractmethod
    async def set_gossip_interval(self, service_name: str, interval: timedelta):
        """
        Sets the gossip interval for the worker.

        Args:
            service_name: the name of the service
            interval: gossip interval
        """
        ...

    @abstractmethod
    async def set_services(self, services: Dict[str, Dict[str, AbstractWorkerProxy]]):
        """
        Sets the available services, and workers for a service.

        Args:
            services: services -> worker_id -> worker
        """
        ...

    @abstractmethod
    async def delete_service(self, service_name: str):
        """
        Deletes a service.

        Args:
            service_name: the name of the service
        """
        ...

    @abstractmethod
    async def pause_service(self, service_name: str):
        """
        Pauses a service routing. Only called by controller.

        Args:
            service_name: the name of the service

        Raises:
            ServiceNotFoundError: if service not found
        """
        ...

    @abstractmethod
    async def unpause_service(self, service_name: str):
        """
        Unpauses a service routing. Only called by controller.

        Args:
            service_name: the name of the service

        Raises:
            ServiceNotFoundError: if service not found
        """
        ...

    @abstractmethod
    async def ferry(self, service_name: str, method: str, session_id: str, data_ref_tuple: Tuple[ObjectRef]) -> Tuple[
        ObjectRef]:
        """
        Forwards a request to the correct worker with the current session. If no session can be found, creates a new one.

        Args:
            service_name: the name of the routed service
            method: the name of the method to be ferried. Only methods not starting with '_' are permitted.
            session_id: the id of the session. No constraints on the shape of this string. Typically, a UUID.
            data_ref_tuple: a tuple of an object ref containing the args to the method. These are resolved on the other
                side, so that the ferry itself never touches the object.

        Returns:
            a tuple of object ref representing the results.

        Raises:
            RetryRouting: if routing should be retried.
            WorkerNotFoundError: if worker not found
            ServiceNotFoundError: if service not found
        """
        ...


class WorkerParams(SerialisableBaseModel):
    """
    A class representing a worker startup entry.
    """
    worker_cls: Type[AbstractStatefulWorker]
    worker_actor_options: Dict[str, Any]
    worker_kwargs: Dict[str, Any]
    max_concurrent_sessions: conint(ge=1)
    expiry_period: timedelta

    @validator('expiry_period')
    def expiry_period_must_be_positive(cls, v):
        if v <= timedelta(seconds=0):
            raise ValueError("Expiry period must be positive.")
        return v


class ServiceParams(SerialisableBaseModel):
    """
    A class representing a worker startup entry.
    """
    worker_params: WorkerParams
    min_num_workers: conint(ge=0)


class AbstractStickRayController(ABC):

    @abstractmethod
    async def shutdown(self):
        """
        Gracefully shutdown controller.
        """
        ...

    @abstractmethod
    async def add_service(self, service_name: str, service_params: ServiceParams):
        """
        Add a service to the registry.

        Args:
            service_name: name of the service
            service_params: ServiceParams for the service
        """
        ...

    @abstractmethod
    async def delete_service(self, service_name: str):
        """
        Delete a service from the registry.

        Args:
            service_name: name of the service
        """
        ...

    @abstractmethod
    async def request_scale_up(self, service_name: str):
        """
        Request scale-up of service. Returns immediately.

        Args:
            service_name: service name
        """
        ...

    @abstractmethod
    async def stake_claim(self, service_name: str, worker_id: str, session_id: str) -> str:
        """
        Stakes a claim for a session, which is granted or denied. This is called when a router wants to session a
        session request to a worker, and the session does not already exist there.

        Args:
            service_name: service name
            worker_id: worker id
            session_id: session id

        Returns:
            the worker id where the claim is at. Returns `worker_id` iff the stake was granted.

        Raises:
            ServiceNotFoundError: if service not found
        """
        ...

    @abstractmethod
    def get_router(self, node_id: str) -> AbstractRouter:
        """
        Gets a router for the given node.

        Args:
            node_id: node id to get router for

        Returns:
            Router on the node
        """
        ...


class AbstractEventBus(ABC):
    """
    Like a Queue except, items are popped by tracking key.
    """

    @abstractmethod
    async def health_check(self):
        ...

    @abstractmethod
    async def size(self) -> int:
        """
        Returns the size of the bucket.

        Returns:
            int, size
        """
        ...

    @abstractmethod
    async def keys(self) -> List[str]:
        """
        Returns a list of keys in bucket.

        Returns:
            list of string keys
        """
        ...

    @abstractmethod
    async def write(self, key: str, item: Any):
        """
        Put an item into the bucket with the given key. Overwrites the existing value with same key, if it exists.

        Args:
            key: str identifier
            item: any object to be stored
        """
        ...

    @abstractmethod
    async def clear(self, key: str):
        """
        Clear the bucket for this key

        Args:
            key: tracking UUID
        """
        ...

    @abstractmethod
    async def pop(self, key: str, timeout: Union[float, None] = None):
        """
        Remove an item from the bucket, optionally blocking and with timeout.

        Args:
            key: tracking UUID
            timeout: float, timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            asyncio.Timeout: if timeout elapsed and item not found
        """
        ...

    @abstractmethod
    async def peek(self, key: str, timeout: Union[float, None] = None):
        """
        Get an item from the bucket, leaving the item there, optionally blocking and with timeout.

        Args:
            key: tracking UUID
            timeout: float, timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            asyncio.Timeout: if timeout elapsed and item not found
        """
        ...


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


class AbstractRoutedServiceHandle(ABC):
    """
    A class representing a handle that can be used to transparently dish out requests to a routed service.
    """

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
    def get_handle(self, sync: bool = True) -> AbstractRoutedServiceHandle:
        """
        Gets a handle for the routed service.

        Args:
            sync: whether it will be a sync or async handle.

        Returns:
            a RoutedServiceHandle or SyncRoutedServiceHandle
        """
        ...


class AbstractBackoffStrategy(ABC):
    """
    A class representing a backoff strategy.
    """

    @abstractmethod
    def __call__(self, attempt: int) -> timedelta:
        """
        Returns the backoff time for the given attempt.

        Args:
            attempt: attempt number

        Returns:
            backoff time
        """
        ...
