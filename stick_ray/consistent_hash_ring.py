import bisect
import hashlib
import logging
from typing import Set, str, Tuple, List, Generator

logger = logging.getLogger(__name__)


class NoAvailableNode(Exception):
    """Exception raised when no node is available."""
    pass


class EmptyRing(Exception):
    """Exception raised when the ring is empty."""
    pass


class ConsistentHashRing:
    """
    A consistent hash ring implementation, based on the one in the Python memcache library.
    """

    def __init__(self, nodes: List[Tuple[str, int]] | None = None, replicas=100):
        self.replicas = replicas
        self.ring = dict()
        self.sorted_keys = []
        if nodes is not None:
            for node, weight in nodes:
                self.add_node(node, weight)

    def _hash(self, key: str):
        """Use SHA-256 as our hash function."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node: str, weight=1):
        """Add a node to the ring with its respective weight."""
        for i in range(self.replicas * weight):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()
        logger.info(f"Added {node} to the ring with weight {weight}")

    def remove_node(self, node: str):
        """Remove a node from the ring."""
        for key in list(self.ring):
            if self.ring[key] == node:
                del self.ring[key]
                self.sorted_keys.remove(key)
        logger.info(f"Removed {node} from the ring")

    def get_node(self, session_id: str) -> str:
        """Get the node responsible for the given session_id using bisect."""
        if not self.ring:
            raise EmptyRing("Hash ring is empty.")

        key = self._hash(session_id)
        position = bisect.bisect_left(self.sorted_keys, key)
        if position == len(self.sorted_keys):
            position = 0
        return self.ring[self.sorted_keys[position]]

    def node_iter(self, session_id: str) -> Generator[str, None, None]:
        """
        Get an iterator over ring.

        Args:
            session_id: session ID

        Returns:
            generator over ring from session's starting point

        Raises:
            EmptyRing if the ring is empty
            NoAvailableNode if all nodes have been looped through.
        """
        if not self.ring:
            raise EmptyRing("Hash ring is empty.")

        key = self._hash(session_id)
        position = bisect.bisect_left(self.sorted_keys, key)

        # We loop to ensure that even if we hit the end of the ring,
        # we can wrap around to the beginning
        start_position = position
        while True:
            if position == len(self.sorted_keys):
                position = 0

            node = self.ring[self.sorted_keys[position]]

            # If the node is not hot, return it. Otherwise, move to the next node.
            yield node
            position += 1

            # If we've circled back to the start position, then all nodes are hot.
            if position == start_position:
                raise NoAvailableNode("All nodes have been tried.")

    def get_next_node(self, session_id: str, hot_nodes: Set[str] | None = None):
        """
        Get the next node responsible for the given session_id using bisect.

        Args:
            session_id: the session ID to be hashed
            hot_nodes: a set of nodes that are currently hot

        Returns:
            the next node responsible for the given session ID, or None if all nodes are hot
        """
        if not self.ring:
            raise EmptyRing("Hash ring is empty.")

        if hot_nodes is None:
            hot_nodes = set()

        key = self._hash(session_id)
        position = bisect.bisect_left(self.sorted_keys, key)

        # We loop to ensure that even if we hit the end of the ring,
        # we can wrap around to the beginning
        start_position = position
        while True:
            if position == len(self.sorted_keys):
                position = 0

            node = self.ring[self.sorted_keys[position]]

            # If the node is not hot, return it. Otherwise, move to the next node.
            if node not in hot_nodes:
                return node
            position += 1

            # If we've circled back to the start position, then all nodes are hot.
            if position == start_position:
                raise NoAvailableNode("All nodes are hot.")
