from datetime import datetime
from typing import Set, TypeVar, Type, Dict, Any

import numpy as np
import ujson
from pydantic import BaseModel

C = TypeVar('C')


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
    def parse_obj(cls: Type[C], obj: Dict[str, Any]) -> C:
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


class LedgerEntry(SerialisableBaseModel):
    worker_id: str
    added_dt: datetime


class BackPressure(Exception):
    """
    An exception raised when a worker is under back pressure.
    """
    pass


class WorkerUpdate(SerialisableBaseModel):
    """
    A class representing a worker update, gossiped to routers.
    """
    utilisation: float  # the capacity of the worker, 0 means empty. 1 means full.
    lazy_downscale: bool  # whether the worker is marked for downscaling soon
    session_ids: Set[str]  # session ids managed by the worker


class WorkerStillBusyError(Exception):
    """
    An exception raised when a worker is still busy, and shutdown requested.
    """
    pass


class SessionNotFound(Exception):
    pass


class RetryRouting(Exception):
    pass


class ServiceNotFoundError(Exception):
    """
    An exception raised when a routed service is is not found regsitered with the router.
    """
    pass


class WorkerNotFoundError(Exception):
    pass


class RoutingFailedError(Exception):
    """
    An exception raised when a routed service is not found regsitered with the router.
    """
    pass


class SessionStateNotFound(Exception):
    """
    Exception raised when a session id is not found.
    """
    pass
