import asyncio
from collections import OrderedDict
from typing import Optional, Any, List, Hashable, Awaitable, Union


async def parallel(*awaitables: Awaitable, return_exceptions: bool = False) -> List[Union[Any, BaseException]]:
    """
    Run multiple requests in parallel.

    Args:
        *awaitables: awaitables to run in parallel
        return_exceptions: if True, return exceptions instead of raising them

    Returns:
        list of results
    """
    refs = await asyncio.gather(
        *awaitables
    )
    results = await asyncio.gather(
        *refs,
        return_exceptions=return_exceptions
    )
    return list(results)


# This is a sentenial value that is used to indicate that no key was provided to the BlockingDict
sentenial = object()


class BlockingDict(object):
    """
    A blocking dictionary that can be used to store and retrieve items in a thread-safe manner.
    """

    def __init__(self):
        self.queue = OrderedDict()  # Used to store items
        self.cv = asyncio.Condition()  # Used to notify other threads that the queue has been updated

    def keys(self) -> List[Hashable]:
        """
        Returns a list of keys in the dictionary.

        Returns:
            list of keys
        """
        return list(self.queue.keys())

    def size(self) -> int:
        """
        Returns the size of the dictionary.

        Returns:
            size
        """
        return len(self.queue)

    async def put(self, key: Hashable, value: Any):
        """
        Put an item into the dictionary with the given key. Overwrites the existing value with same key, if it exists.

        Args:
            key: key
            value: value
        """
        async with self.cv:
            self.queue[key] = value
            self.cv.notify_all()

    async def pop(self, key: Optional[Hashable] = sentenial, timeout: Optional[float] = None) -> Any:
        """
        Remove an item from the dictionary, optionally blocking and with timeout.

        Args:
            key: key
            timeout: timeout in seconds to wait when blocking.

        Returns:
            item matching key or first item if no key is provided

        Raises:
            asyncio.Timeout if timeout elapsed and item not found
        """

        async def _pop():
            async with self.cv:
                if key is sentenial:
                    await self.cv.wait_for(lambda: len(self.queue) > 0)
                    return self.queue.pop(next(iter(self.queue.keys())))
                else:
                    await self.cv.wait_for(lambda: key in self.queue)
                    return self.queue.pop(key)

        if timeout is not None:
            return await asyncio.wait_for(_pop(), timeout=timeout)
        return await _pop()

    async def peek(self, key: Hashable, timeout: Optional[float] = None):
        """
        Get an item from the dictionary, leaving the item there, optionally blocking and with timeout.

        Args:
            key: key
            timeout: timeout in seconds to wait when blocking.

        Returns:
            item matching key

        Raises:
            asyncio.Timeout if timeout elapsed and item not found
        """

        async def _pop():
            async with self.cv:
                await self.cv.wait_for(lambda: key in self.queue)
                return self.queue[key]

        if timeout is not None:
            return await asyncio.wait_for(_pop(), timeout=timeout)
        return await _pop()

    async def has(self, key: Hashable) -> bool:
        """
        Check if the dictionary contains the given key.

        Args:
            key: key

        Returns:
            True if key is in dictionary, False otherwise
        """
        return key in self.queue

    async def delete(self, key: Hashable):
        """
        Delete the given key from the dictionary.

        Args:
            key: key
        """
        if key in self.queue:
            del self.queue[key]
