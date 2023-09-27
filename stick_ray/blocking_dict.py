import asyncio
from typing import Optional, Any, List, Dict


class BlockingDict(object):
    """
    A blocking dictionary that can be used to store and retrieve items in a thread-safe manner.
    """

    def __init__(self):
        self._conditions: Dict[str, asyncio.Condition] = dict()  # Used to store condition variables
        self._data: Dict[str, Any] = dict()  # Used to store items
        self._lock = asyncio.Lock()  # Used to lock the dictionary

    def keys(self) -> List[str]:
        """
        Returns a list of keys in the dictionary.

        Returns:
            list of keys
        """
        return list(self._data.keys())

    def size(self) -> int:
        """
        Returns the size of the dictionary.

        Returns:
            size
        """
        return len(self._data)

    async def put(self, key: str, value: Any):
        """
        Put an item into the dictionary with the given key. Overwrites the existing value with same key, if it exists.

        Args:
            key: key
            value: value
        """
        cv: asyncio.Condition
        async with self._lock:
            if key not in self._conditions:
                # New lock is created for new key which may be overhead (should test)
                self._conditions[key] = asyncio.Condition()
            cv = self._conditions[key]  # Get this reference before releasing lock
        async with cv:
            self._data[key] = value
            cv.notify_all()

    async def peek(self, key: str, timeout: Optional[float] = None) -> Any:
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
        cv: asyncio.Condition
        async with self._lock:
            if key not in self._conditions:
                # New lock is created which may be overhead (should test)
                self._conditions[key] = asyncio.Condition()
            cv = self._conditions[key]  # Get this reference before releasing lock

        async with cv:
            async def _peek():
                await cv.wait_for(lambda: key in self._data)
                return self._data[key]

            if timeout is not None:
                return await asyncio.wait_for(_peek(), timeout=timeout)
            return await _peek()

    def has(self, key: str) -> bool:
        """
        Check if the dictionary contains the given key.

        Args:
            key: key

        Returns:
            True if key is in dictionary, False otherwise
        """
        return key in self._data

    async def delete(self, key: str):
        """
        Delete the given key from the dictionary.

        Args:
            key: key
        """
        cv: asyncio.Condition
        async with self._lock:
            if key not in self._conditions:
                # New lock is created which may be overhead (should test)
                self._conditions[key] = asyncio.Condition()
            cv = self._conditions[key]  # Get this reference before releasing lock

        async with cv:
            # Get lock on this condition, prevents a delete from happening while another thread has this lock.

            # Delete the data
            if key in self._data:
                del self._data[key]
                cv.notify_all()
