import pytest

from stick_ray.consistent_hash_ring import ConsistentHashRing, NoAvailableNode, EmptyRing


def test_add_node():
    ring = ConsistentHashRing()
    ring.add_node("worker_1")
    assert len(ring.ring) == ring.replicas
    assert len(ring.sorted_keys) == ring.replicas
    assert all(isinstance(key, int) for key in ring.sorted_keys)


def test_remove_node():
    ring = ConsistentHashRing()
    ring.add_node("worker_1")
    ring.remove_node("worker_1")
    assert len(ring.ring) == 0
    assert len(ring.sorted_keys) == 0


def test_get_node_empty_ring():
    ring = ConsistentHashRing()
    with pytest.raises(EmptyRing, match="Hash ring is empty."):
        ring.get_node("session_1")


def test_get_node_existing_nodes():
    ring = ConsistentHashRing()
    ring.add_node("worker_1")
    ring.add_node("worker_2")
    assigned_worker = ring.get_node("session_1")
    assert assigned_worker in ["worker_1", "worker_2"]

    # With consistent hashing, the same session ID should always be routed to the same worker
    assert ring.get_node("session_1") == assigned_worker


def test_distribution():
    """Tests if nodes (workers) receive roughly equal distribution."""
    ring = ConsistentHashRing()
    ring.add_node("worker_1")
    ring.add_node("worker_2")
    count = {"worker_1": 0, "worker_2": 0}
    for i in range(1000):
        worker = ring.get_node(f"session_{i}")
        count[worker] += 1

    # Roughly equal distribution
    assert abs(count["worker_1"] - count["worker_2"]) <= 100  # This threshold can be adjusted based on requirements.


def test_remove_and_add_back():
    ring = ConsistentHashRing()
    ring.add_node("worker_1")
    ring.add_node("worker_2")
    worker_for_session_1 = ring.get_node("session_1")

    ring.remove_node(worker_for_session_1)
    ring.add_node(worker_for_session_1)

    # The session should still be assigned to the same worker even after removal and re-adding
    assert ring.get_node("session_1") == worker_for_session_1


def test_basic_next_node_functionality():
    nodes = [("worker_1", 1), ("worker_2", 1)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    hot_nodes = {"worker_1"}
    assert ring.get_next_node("some_session_id", hot_nodes) == "worker_2"

    hot_nodes = {"worker_2"}
    assert ring.get_next_node("some_session_id", hot_nodes) == "worker_1"


def test_all_nodes_hot():
    nodes = [("worker_1", 1), ("worker_2", 1)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    hot_nodes = {"worker_1", "worker_2"}
    with pytest.raises(NoAvailableNode):
        ring.get_next_node("some_session_id", hot_nodes)


def test_wrap_around_ring():
    nodes = [("worker_1", 1), ("worker_2", 1)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    # Assuming "last_session_id" hashes to the end of the ring or close to it,
    # the next node should wrap around to the beginning of the ring.
    # For this test, we're assuming that worker_2 is at the end of the ring (which might not always be the case).
    hot_nodes = {"worker_2"}
    assert ring.get_next_node("last_session_id", hot_nodes) == "worker_1"


@pytest.mark.parametrize(
    "session_id,expected_node",
    [("session1", "worker_2"), ("session3", "worker_2")]
)
def test_multiple_sessions(session_id, expected_node):
    nodes = [("worker_1", 1), ("worker_2", 1)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    hot_nodes = {"worker_1"}
    assert ring.get_next_node(session_id, hot_nodes) == expected_node


def generate_distributions(ring, num_keys=100000):
    """Hash num_keys random keys and track distribution across nodes."""
    distributions = {}
    for _ in range(num_keys):
        node = ring.get_node(str(_))
        distributions[node] = distributions.get(node, 0) + 1
    return distributions


def test_weighted_distribution():
    nodes = [("worker_1", 1), ("worker_2", 3), ("worker_3", 6)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    distributions = generate_distributions(ring)

    # The distributions should be roughly in proportion to the weights.
    # Note: Due to the nature of random distribution and hashing, it won't be exact.
    total = sum(distributions.values())
    assert 0.09 < distributions["worker_1"] / total < 0.11  # roughly 10%
    assert 0.29 < distributions["worker_2"] / total < 0.31  # roughly 30%
    assert 0.59 < distributions["worker_3"] / total < 0.61  # roughly 60%


def test_zero_weight_distribution():
    nodes = [("worker_1", 1), ("worker_2", 0), ("worker_3", 1)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    distributions = generate_distributions(ring)

    # Worker_2 with 0 weight should have 0 distribution.
    assert "worker_2" not in distributions or distributions["worker_2"] == 0


def test_negative_weight_distribution():
    """Nodes with negative weights should be treated as having zero weight."""
    nodes = [("worker_1", 1), ("worker_2", -1), ("worker_3", 1)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    distributions = generate_distributions(ring)

    assert "worker_2" not in distributions or distributions["worker_2"] == 0


def test_single_weight_distribution():
    """All keys should go to the sole worker with positive weight."""
    nodes = [("worker_1", 0), ("worker_2", 5), ("worker_3", 0)]
    ring = ConsistentHashRing()
    for node in nodes:
        ring.add_node(node[0], node[1])

    distributions = generate_distributions(ring, num_keys=100000)

    assert distributions["worker_2"] == 100000
    assert "worker_1" not in distributions
    assert "worker_3" not in distributions


def test_idempotency_of_adding_nodes():
    nodes_initial = [("worker_1", 1), ("worker_2", 3), ("worker_3", 6)]
    ring_initial = ConsistentHashRing()
    for node in nodes_initial:
        ring_initial.add_node(node[0], node[1])

    distributions_initial = generate_distributions(ring_initial)

    # Add an existing node (essentially doing nothing)
    nodes_new = [("worker_1", 1), ("worker_2", 3), ("worker_3", 6), ("worker_2", 3)]
    ring_new = ConsistentHashRing()
    for node in nodes_new:
        ring_new.add_node(node[0], node[1])

    distributions_new = generate_distributions(ring_new)

    # The distributions should be the same before and after adding the existing node
    assert distributions_initial == distributions_new


def test_distribution_change_with_weight_update():
    nodes_initial = [("worker_1", 1), ("worker_2", 3), ("worker_3", 6)]
    ring_initial = ConsistentHashRing()
    for node in nodes_initial:
        ring_initial.add_node(node[0], node[1])

    distributions_initial = generate_distributions(ring_initial)

    # Change weight of an existing node
    nodes_updated = [("worker_1", 1), ("worker_2", 5), ("worker_3", 6)]  # worker_2's weight is changed from 3 to 5
    ring_updated = ConsistentHashRing()
    for node in nodes_updated:
        ring_updated.add_node(node[0], node[1])

    distributions_updated = generate_distributions(ring_updated)

    # The distributions should have changed for worker_2 since its weight has increased
    total_initial = sum(distributions_initial.values())
    total_updated = sum(distributions_updated.values())

    proportion_initial = distributions_initial["worker_2"] / total_initial
    proportion_updated = distributions_updated["worker_2"] / total_updated

    assert proportion_initial < proportion_updated


@pytest.fixture
def setup_ring():
    nodes = [("node1", 1), ("node2", 2), ("node3", 1)]
    ring = ConsistentHashRing(nodes)
    return ring


def test_node_iter_yields_nodes_in_order(setup_ring):
    session_id = "some_session_id"
    gen = setup_ring.node_iter(session_id)

    nodes = [next(gen) for _ in range(3)]

    with pytest.raises(NoAvailableNode):
        next(gen)

    assert "node1" in nodes
    assert "node2" in nodes
    assert "node3" in nodes


def test_node_iter_raises_empty_ring_exception():
    ring = ConsistentHashRing()
    with pytest.raises(EmptyRing):
        _ = next(ring.node_iter("some_session_id"))


def test_node_iter_raises_no_available_node_exception(setup_ring):
    session_id = "some_session_id"
    nodes_iter = setup_ring.node_iter(session_id)

    # Exhaust the iterator
    for _ in range(3):
        next(nodes_iter)

    with pytest.raises(NoAvailableNode):
        next(nodes_iter)


def test_node_iter_wraps_around(setup_ring):
    # Get a session_id that will be close to the end of the ring's keys
    for node in set(setup_ring.ring.values()):
        nodes_iter = setup_ring.node_iter(node)
        for _ in range(3):
            next(nodes_iter)
        with pytest.raises(NoAvailableNode):
            next(nodes_iter)
