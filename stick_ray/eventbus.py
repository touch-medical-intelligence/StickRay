import logging
from typing import Any, List, Type, Hashable, Union

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.blocking_dict import BlockingDict
from stick_ray.namespace import NAMESPACE

logger = logging.getLogger(__name__)


class EventBus:
    """
    An event bus for passing messages destined for multiple listeners.
    """

    def __init__(self, name: str):
        head_node_id = ray.get_runtime_context().get_node_id()
        event_bus_name = self.event_bus_name(name)

        try:
            event_bus = ray.get_actor(event_bus_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {event_bus_name}")
        except ValueError:
            event_bus_actor_options = {
                "num_cpus": 0,
                "name": event_bus_name,
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

            dynamic_cls = self.dynamic_cls(name)

            event_bus_kwargs = dict()
            event_bus = ray.remote(dynamic_cls).options(**event_bus_actor_options).remote(**event_bus_kwargs)
            ray.get(event_bus.health_check.remote())
            logger.info(f"Created new {event_bus_name}")
        self._actor = event_bus

    @staticmethod
    def dynamic_cls(name: str) -> Type:
        """
        Create a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.

        Args:
            name: name of the event bus

        Returns:
            a dynamic class
        """
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"EventBus:{name}",
            (_EventBus,),
            dict(_EventBus.__dict__),
        )

    @staticmethod
    def event_bus_name(name: str) -> str:
        return f"{name}.event_bus"

    async def size(self) -> int:
        """
        Returns the size of the bucket.

        Returns:
            int, size
        """
        return await self._actor.size()

    async def keys(self) -> List[str]:
        """
        Returns a list of keys in bucket.

        Returns:
            list of string keys
        """
        return await self._actor.keys()

    async def write(self, key: Hashable, item: Any):
        """
        Store an item with a given key.

        Args:
            key:
            item:

        Returns:

        """
        await self._actor.write.remote(key, item)

    async def clear(self, key: Hashable):
        """
        Clear the bucket of all items for this user

        Args:
            key: tracking UUID
        """
        await self._actor.clear.remote(key)

    async def pop(self, key: Hashable, timeout: Union[float, None] = None):
        """
        Remove an item from the bucket, optionally blocking and with timeout.

        Args:
            key: tracking UUID
            timeout: float, timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            Timeout if block=True and timeout elapsed and item not found
            NotFound if block=False and item not found
        """
        return await self._actor.pop.remote(key, timeout)

    async def peek(self, key: Hashable, timeout: Union[float, None] = None):
        """
        Get an item from the bucket, leaving the item there, optionally blocking and with timeout.

        Args:
            key: tracking UUID
            timeout: float, timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            Timeout if block=True and timeout elapsed and item not found
            NotFound if block=False and item not found
        """
        return await self._actor.peek.remote(key, timeout)


class _EventBus:
    """
    Like a Queue except, items are popped by tracking key.
    """

    def __init__(self):
        self.items = BlockingDict()

    async def health_check(self):
        return

    async def size(self) -> int:
        """
        Returns the size of the bucket.

        Returns:
            int, size
        """
        return self.items.size()

    async def keys(self) -> List[Hashable]:
        """
        Returns a list of keys in bucket.

        Returns:
            list of string keys
        """
        return self.items.keys()

    async def write(self, key: Hashable, item: Any):
        """
        Put an item into the bucket with the given key. Overwrites the existing value with same key, if it exists.

        Args:
            key: Hashable identifier
            item: any object to be stored
        """
        await self.items.put(key, item)

    async def clear(self, key: Hashable):
        """
        Clear the bucket for this key

        Args:
            key: tracking UUID
        """
        await self.items.delete(key)

    async def pop(self, key: Hashable, timeout: Union[float, None] = None):
        """
        Remove an item from the bucket, optionally blocking and with timeout.

        Args:
            key: tracking UUID
            timeout: float, timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            asyncio.Timeout if timeout elapsed and item not found
        """
        return await self.items.pop(key, timeout)

    async def peek(self, key: Hashable, timeout: Union[float, None] = None):
        """
        Get an item from the bucket, leaving the item there, optionally blocking and with timeout.

        Args:
            key: tracking UUID
            timeout: float, timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            asyncio.Timeout if timeout elapsed and item not found
        """
        return await self.items.peek(key, timeout)
