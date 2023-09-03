import asyncio
import hashlib
import inspect
import uuid
from datetime import datetime, tzinfo
from typing import Dict, Any, Callable, Union

import numpy as np
import ujson
from pydantic import BaseModel


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get a running async event loop if one exists, otherwise create one.

    This function serves as a proxy for the deprecating get_event_loop().
    It tries to get the running loop first, and if no running loop
    could be retrieved:
    - For python version <3.10: it falls back to the get_event_loop
        call.
    - For python version >= 3.10: it uses the same python implementation
        of _get_event_loop() at asyncio/events.py.

    Ideally, one should use high level APIs like asyncio.run() with python
    version >= 3.7, if not possible, one should create and manage the event
    loops explicitly.
    """
    import sys

    vers_info = sys.version_info
    if vers_info.major >= 3 and vers_info.minor >= 10:
        # This follows the implementation of the deprecating `get_event_loop`
        # in python3.10's asyncio. See python3.10/asyncio/events.py
        # _get_event_loop()
        try:
            loop = asyncio.get_running_loop()
            assert loop is not None
            return loop
        except RuntimeError as e:
            # No running loop, relying on the error message as for now to
            # differentiate runtime errors.
            if "no running event loop" in str(e):
                return asyncio.get_event_loop_policy().get_event_loop()
            else:
                raise e

    return asyncio.get_event_loop()


def deterministic_uuid(seed: str) -> uuid.UUID:
    """
    Generate a UUID using a deterministic hashing of a seed string.

    Args:
        seed: str, a string seed

    Returns:
        UUID

    """
    if not isinstance(seed, str):
        raise TypeError(f"Expected seed type `str`, got {type(seed)}.")
    m = hashlib.md5()
    m.update(seed.encode('utf-8'))
    new_uuid = uuid.UUID(m.hexdigest())
    return new_uuid


class SerialisableBaseModel(BaseModel):
    """
    A pydantic BaseModel that can be serialised and deserialised using pickle, working well with Ray.
    """

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True
        json_loads = ujson.loads  # can use because ujson decodes NaN and Infinity
        json_dumps = ujson.dumps  # (currently not possible because ujson doesn't encode NaN and Infinity like json)
        # json_dumps = lambda *args, **kwargs: json.dumps(*args, **kwargs, separators=(',', ':'))
        json_encoders = {np.ndarray: lambda x: x.tolist()}

    @classmethod
    def _deserialise(cls, kwargs):
        """Required for this class's __reduce__ method to be picklable."""
        return cls(**kwargs)

    @classmethod
    def parse_obj(cls, obj: Dict[str, Any]) -> 'BaseModel':
        model_fields = cls.__fields__  # get fields of the model

        # Convert all fields that are defined as np.ndarray
        for name, field in model_fields.items():
            if isinstance(field.type_, type) and issubclass(field.type_, np.ndarray):
                if name in obj and isinstance(obj[name], list):
                    obj[name] = np.array(obj[name])

        return super().parse_obj(obj)

    def __reduce__(self):
        # Uses the dict representation of the model to serialise and deserialise.
        # The efficiency of this depends on the efficiency of the dict representation serialisation.
        serialised_data = self.dict()
        return self.__class__._deserialise, (serialised_data,)


def set_datetime_timezone(dt: datetime, offset: Union[str, tzinfo]) -> datetime:
    """
    Replaces the datetime object's timezone with one from an offset.

    Args:
        dt: datetime, with out without a timezone set. If set, will be replaced.
        offset: tzinfo, or str offset like '-04:00' (which means EST)

    Returns:
        datetime with timezone set
    """
    if isinstance(offset, str):
        dt = dt.replace(tzinfo=None)
        return datetime.fromisoformat(f"{dt.isoformat()}{offset}")
    if isinstance(offset, tzinfo):
        return dt.replace(tzinfo=offset)
    raise ValueError(f"offset {offset} not understood.")


def current_utc() -> datetime:
    """
    Get the current datetime in UTC, with timezone set to UTC.

    Returns:
        datetime
    """
    return set_datetime_timezone(datetime.utcnow(), '+00:00')


def is_key_after_star(func: Callable, key: str):
    """
    Checks if a function has a keyword-only argument after a star argument.

    Args:
        func: Callable to check for keyword-only argument after star argument.
        key: keyword-only argument to check for.

    Returns:
        True if key is a keyword-only argument after star argument.
    """
    signature = inspect.signature(func)
    parameters = signature.parameters

    for name, param in parameters.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY and name == key:
            return True
    return False
