import pickle
from abc import ABC, abstractmethod

import ray

from stick_ray.actor_interface import ActorInterface


class AbstractExample(ABC):
    @abstractmethod
    def f(self, x):
        ...

    @abstractmethod
    def g(self, x):
        ...


class Example(AbstractExample, ActorInterface):
    def __init__(self):
        actor = ray.remote(_Example).remote()
        ActorInterface.__init__(self, actor=actor)


class _Example(AbstractExample):
    def f(self, x):
        return x

    def g(self, x):
        return x + 1


def test_serialisation():
    example = Example()
    example_bytes = pickle.dumps(example)
    new_example: Example = pickle.loads(example_bytes)
    assert isinstance(new_example, Example)


def test_actor_interface_evaluation():
    example = Example()

    assert example.f(x=1) == 1
    assert example.g(x=1) == 2
