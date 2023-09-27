import inspect
from abc import update_abstractmethods
from typing import Callable

import ray
from ray.actor import ActorHandle, ActorMethod

from stick_ray.abc import AbstractActorInterface


def _create_actor_method_caller(method_name: str, abstract_func: Callable):
    """
    Create a method that calls the actor method with the same name.

    Args:
        method_name: method name
        abstract_func: abstract method from ABC

    Returns:
        a method that calls the actor method with the same name.
    """
    if inspect.iscoroutinefunction(abstract_func):
        async def f(self, *args, **kwargs):
            actor_method = getattr(self._actor, method_name, None)
            if not isinstance(actor_method, ActorMethod):
                raise TypeError(f"Expected ActorMethod, got {type(actor_method)}.")
            return await actor_method.remote(*args, **kwargs)
    else:
        def f(self, *args, **kwargs):
            actor_method = getattr(self._actor, method_name, None)
            if not isinstance(actor_method, ActorMethod):
                raise TypeError(f"Expected ActorMethod, got {type(actor_method)}.")
            return ray.get(actor_method.remote(*args, **kwargs))
    return f


class ActorInterface(AbstractActorInterface):
    """
    A base class for actor interfaces. This class is never invoked directly. Handles serialization and deserialization.
    """

    def __init__(self, actor: ActorHandle):
        if not isinstance(actor, ActorHandle):
            raise ValueError("`actor` should be an instance of ActorHandle")
        self._actor = actor

    def __new__(cls, *args, **kwargs):
        _deserialize = kwargs.pop('_deserialize', False)
        if _deserialize and 'actor' not in kwargs:
            raise ValueError("Deserializing requires an 'actor' argument")

        # Since we ferry everything to the actor, if something is not implemented, it'll crash there.
        # Hence we remove all abstract methods from this class, before making the instance.
        for name in getattr(cls, '__abstractmethods__', ()):
            value = getattr(cls, name, None)

            if getattr(value, "__isabstractmethod__", False):
                # All abstract methods can replaced with pointers to actor method
                setattr(cls, name, _create_actor_method_caller(method_name=name, abstract_func=value))
        # Also add any other newly added abstract methods.
        for name, value in cls.__dict__.items():

            if getattr(value, "__isabstractmethod__", False):
                setattr(cls, name, _create_actor_method_caller(method_name=name, abstract_func=value))
        update_abstractmethods(cls)

        # Check if deserializing
        if _deserialize:
            instance = super(ActorInterface, cls).__new__(cls)
            instance._actor = kwargs.get('actor')
        else:
            instance = super(ActorInterface, cls).__new__(cls)
        return instance

    def __reduce__(self):
        # Return the class method for deserialization and the actor as an argument
        serialised_data = dict(
            actor=self._actor
        )
        return (self._deserialise, (serialised_data,))

    @classmethod
    def _deserialise(cls, kwargs):
        # Create a new instance, bypassing __init__ and setting the actor directly
        return cls.__new__(cls, _deserialize=True, **kwargs)
