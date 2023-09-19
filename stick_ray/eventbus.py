import logging
from typing import Any, List, Type, Hashable, Union

import ray
from ray.actor import ActorHandle
from ray.serve._private.utils import get_head_node_id
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from stick_ray.blocking_dict import BlockingDict
from stick_ray.namespace import NAMESPACE

logger = logging.getLogger(__name__)


class BaseEventBus:
    def __init__(self, actor: ActorHandle):
        self._actor = actor

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return BaseEventBus(**kwargs)

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        serialised_data = dict(
            actor=self._actor,
        )
        return self.__class__._deserialise, (serialised_data,)


class EventBus(BaseEventBus):
    """
    An event bus for passing messages between those that care.
    """

    def __init__(self):
        actor_name = self.actor_name()

        try:
            actor = ray.get_actor(actor_name, namespace=NAMESPACE)
            logger.info(f"Connected to existing {actor_name}")
        except ValueError:
            try:
                placement_node_id = get_head_node_id()
            except:
                placement_node_id = ray.get_runtime_context().get_node_id()
            actor_options = {
                "num_cpus": 0,
                "name": actor_name,
                "lifetime": "detached",
                "max_restarts": -1,
                "max_task_retries": -1,
                # Schedule the controller on the head node with a soft constraint. This
                # prefers it to run on the head node in most cases, but allows it to be
                # restarted on other nodes in an HA cluster.
                "scheduling_strategy": NodeAffinitySchedulingStrategy(placement_node_id, soft=True),
                "namespace": NAMESPACE,
                "max_concurrency": 15000  # Needs to be large, as there should be no limit.
            }

            dynamic_cls = self.dynamic_cls()

            actor = ray.remote(dynamic_cls).options(**actor_options).remote()
            ray.get(actor.health_check.remote())
            logger.info(f"Created new {actor_name}")
        super().__init__(actor=actor)

    @staticmethod
    def dynamic_cls() -> Type:
        """
        Create a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.

        Args:
            name: name of the event bus

        Returns:
            a dynamic class
        """
        # a dynamic class that will be parsed properly by ray dashboard, so that it has a nice class name.
        return type(
            f"StickRayEventBus",
            (_EventBus,),
            dict(_EventBus.__dict__),
        )

    @staticmethod
    def actor_name() -> str:
        return f"STICK_RAY_EVENT_BUS_ACTOR"

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

    def health_check(self):
        return

    def size(self) -> int:
        """
        Returns the size of the bucket.

        Returns:
            int, size
        """
        return self.items.size()

    def keys(self) -> List[Hashable]:
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
        value = await self.items.peek(key, timeout)
        await self.items.delete(key)
        return value

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
