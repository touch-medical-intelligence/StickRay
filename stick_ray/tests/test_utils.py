import asyncio
import pickle
import sys
import uuid
from collections import namedtuple
from datetime import datetime, timedelta, timezone, tzinfo
from unittest.mock import AsyncMock, patch, call

import numpy as np
import pytest
import ujson
from pydantic import ValidationError, BaseModel

from stick_ray.utils import deterministic_uuid, get_or_create_event_loop, set_datetime_timezone, \
    is_key_after_star, ValueMonitor, IdentifyBackoffStrategy
from stick_ray.common import SerialisableBaseModel

VersionInfo = namedtuple("VersionInfo", "major minor micro releaselevel serial")


def test_deterministic_uuid():
    assert isinstance(deterministic_uuid('hello world'), uuid.UUID)
    assert deterministic_uuid('hello world') != deterministic_uuid('hello-world')


def test_validate_assignment():
    class A(SerialisableBaseModel):
        a: bool

    a = A(a=True)
    a.a = 0
    a.a = 1
    with pytest.raises(ValidationError):
        a.a = 2


def test_serialisable_base_model():
    class A(BaseModel):
        a: float
        b: float
        c: float

    # success!
    s = A(a=np.nan, b=np.inf, c=-np.inf)
    t = A(a=np.nan, b=np.inf, c=-np.inf)
    assert s == t

    # # fail! Expected comparison to work
    # s = A(a=float('nan'), b=float('inf'), c=-float('inf'))
    # t = A(a=float('nan'), b=float('inf'), c=-float('inf'))
    # assert s == t


@pytest.mark.asyncio
async def test_get_running_loop():
    # Here we're trying to see if it correctly retrieves an active event loop.
    loop = asyncio.get_event_loop()
    assert get_or_create_event_loop() == loop


def test_python_version_less_than_3_10(monkeypatch):
    # Simulating python version <3.10
    monkeypatch.setattr(sys, "version_info", VersionInfo(3, 9, 0, 'final', 0))

    # You might want to add some logic here to ensure no running loop.
    # For simplicity, we're assuming there's no running loop at the moment.

    loop = get_or_create_event_loop()
    assert isinstance(loop, asyncio.AbstractEventLoop)


def test_python_version_gte_3_10_without_running_loop(monkeypatch):
    # Simulating python version >=3.10 and no running loop.
    monkeypatch.setattr(sys, "version_info", VersionInfo(3, 10, 0, 'final', 0))

    # Here we are mocking the get_running_loop to raise an exception.
    # This simulates the scenario where there isn't a running event loop.
    def mock_get_running_loop():
        raise RuntimeError("no running event loop")

    monkeypatch.setattr(asyncio, "get_running_loop", mock_get_running_loop)

    loop = get_or_create_event_loop()
    assert isinstance(loop, asyncio.AbstractEventLoop)


def test_python_version_gte_3_10_with_different_exception(monkeypatch):
    # Simulating python version >=3.10 and a different RuntimeError.
    monkeypatch.setattr(sys, "version_info", VersionInfo(3, 10, 0, 'final', 0))

    def mock_get_running_loop():
        raise RuntimeError("some different error")

    monkeypatch.setattr(asyncio, "get_running_loop", mock_get_running_loop)

    with pytest.raises(RuntimeError, match="some different error"):
        get_or_create_event_loop()


class CustomTimeZone(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=-5)

    def dst(self, dt):
        return timedelta(0)


def test_set_timezone_with_str_offset():
    dt = datetime(2022, 1, 1, 12)
    result = set_datetime_timezone(dt, '-04:00')
    assert result.tzinfo.utcoffset(None) == timedelta(hours=-4)


def test_set_timezone_with_tzinfo():
    dt = datetime(2022, 1, 1, 12)
    custom_tz = CustomTimeZone()
    result = set_datetime_timezone(dt, custom_tz)
    assert result.tzinfo == custom_tz


def test_unsupported_offset_type():
    dt = datetime(2022, 1, 1, 12)
    with pytest.raises(ValueError, match=r"offset \d+ not understood."):
        set_datetime_timezone(dt, 1234)


def test_replace_existing_timezone():
    dt = datetime(2022, 1, 1, 12, tzinfo=timezone.utc)
    result = set_datetime_timezone(dt, '-04:00')
    assert result.tzinfo.utcoffset(None) == timedelta(hours=-4)


class TestModelInt(SerialisableBaseModel):
    value: int


def test_serialise_deserialise_model():
    model = TestModelInt(value=10)
    serialized_data = pickle.dumps(model)
    deserialized_model = pickle.loads(serialized_data)

    assert isinstance(deserialized_model, TestModelInt)
    assert deserialized_model.value == model.value


def test_config_values():
    assert TestModelInt.Config.validate_assignment is True
    assert TestModelInt.Config.arbitrary_types_allowed is True
    assert TestModelInt.Config.json_loads == ujson.loads
    # You can test for json_dumps once you decide on its implementation
    assert isinstance(TestModelInt.Config.json_encoders[np.ndarray], type(lambda x: x))


class TestModelNp(SerialisableBaseModel):
    array: np.ndarray


def test_numpy_array_json_serialization():
    model = TestModelNp(array=np.array([1, 2, 3]))
    serialized_data = model.json()

    expected_json = '{"array":[1,2,3]}'
    assert serialized_data == expected_json

    # Deserialize from the serialized data
    deserialized_model = TestModelNp.parse_raw(serialized_data)

    # Assert that the reconstructed numpy array is correct
    np.testing.assert_array_equal(deserialized_model.array, model.array)


def test_is_key_after_star():
    # Example usage:
    def f1(a, b, *, session_id):
        pass

    def f2(a, b, session_id):
        pass

    def f3(a, b):
        pass

    def f4(a, b, session_id=None):
        pass

    def f5(a, b, *, k, session_id=None):
        pass

    def f6(a, b, *, session_id, cache_key):
        pass

    assert is_key_after_star(f1, "session_id")
    assert not is_key_after_star(f1, "a")
    assert not is_key_after_star(f2, "session_id")
    assert not is_key_after_star(f3, "session_id")
    assert not is_key_after_star(f4, "session_id")
    assert is_key_after_star(f5, "session_id")
    assert is_key_after_star(f6, "session_id")
    assert is_key_after_star(f6, "cache_key")



@pytest.mark.asyncio
async def test_value_changes():
    # Mocks
    coroutine_mock = AsyncMock(side_effect=[1, 2, 3])
    callback_mock = AsyncMock()

    monitor = ValueMonitor(coroutine_mock, callback_mock, timedelta(seconds=0.1))
    await monitor.start()
    await asyncio.sleep(0.4)  # Give some time for the monitoring loop to fetch the values
    await monitor.stop()

    callback_mock.assert_has_calls([call(1), call(2), call(3)])

@pytest.mark.asyncio
async def test_value_remains_same():
    coroutine_mock = AsyncMock(return_value=1)
    callback_mock = AsyncMock()

    monitor = ValueMonitor(coroutine_mock, callback_mock, timedelta(seconds=0.1))
    await monitor.start()
    await asyncio.sleep(0.4)  # Give some time for the monitoring loop to fetch the values
    await monitor.stop()

    callback_mock.assert_called_once_with(1)

@pytest.mark.asyncio
async def test_error_backoff_strategy():
    # The coroutine_mock is made to raise an error
    coroutine_mock = AsyncMock(side_effect=Exception("Sample exception"))
    callback_mock = AsyncMock()
    error_strategy_mock = IdentifyBackoffStrategy()

    with patch.object(error_strategy_mock, '__call__', return_value=timedelta(seconds=0.1)) as strategy_mock:
        monitor = ValueMonitor(coroutine_mock, callback_mock, timedelta(seconds=0.1), error_strategy_mock)
        await monitor.start()
        await asyncio.sleep(0.5)  # Give some time for the monitoring loop to attempt fetching the values multiple times
        await monitor.stop()

    # Since our coroutine_mock raises an error every time, and the strategy is set to sleep 0.1s on errors,
    # the strategy should have been called multiple times in 0.5s
    assert strategy_mock.call_count > 3

@pytest.mark.asyncio
async def test_start_and_stop():
    coroutine_mock = AsyncMock(return_value=1)
    callback_mock = AsyncMock()

    monitor = ValueMonitor(coroutine_mock, callback_mock, timedelta(seconds=0.1))
    await monitor.start()
    assert monitor.monitor_task is not None

    await monitor.stop()
    assert monitor.monitor_task is None