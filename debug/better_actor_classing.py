import pickle
from abc import abstractmethod, ABC


class BaseActorInterface:
    def __init__(self, actor):
        self._actor = actor

    def __getattr__(self, name):
        """Redirect the call to the corresponding method of the encapsulated actor."""
        actor_method = getattr(self._actor, name, None)
        if callable(actor_method):
            return actor_method
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __new__(cls, *args, **kwargs):
        _deserialize = kwargs.pop('_deserialize', False)
        if _deserialize and 'actor' not in kwargs:
            raise ValueError("Deserializing requires an 'actor' argument")

        # Check if deserializing
        if _deserialize:
            instance = super(BaseActorInterface, cls).__new__(cls)
            instance._actor = kwargs.get('actor')
            return instance
        else:
            return super(BaseActorInterface, cls).__new__(cls)

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


class DummyActor:
    def __init__(self, some_arg):
        self.some_arg = some_arg

    def method(self, x):
        return (x, self.some_arg)


class AbstractExample(ABC):
    @abstractmethod
    def method(self, x):
        """
        Some method.
        """
        ...


class _Example(AbstractExample):

    def method(self, x):
        pass


class Example(BaseActorInterface, AbstractExample):
    def __init__(self, some_arg):
        BaseActorInterface.__init__(self, actor=DummyActor(some_arg=some_arg))

    def method(self, x):
        pass


if __name__ == '__main__':
    c = Example("hello")
    class_bytes = pickle.dumps(c)
    c2 = pickle.loads(class_bytes)
    assert isinstance(c2, Example)
