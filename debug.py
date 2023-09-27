import asyncio
from datetime import timedelta

import numpy as np
import pytest
import ray
from stick_ray.utils import get_or_create_event_loop

from stick_ray.common import ServiceNotFoundError

from stick_ray import routed_service
from stick_ray.stateful_worker import StatefulWorker


@routed_service(expiry_period=timedelta(seconds=10), max_concurrent_sessions=2)
class ToyWorker(StatefulWorker):
    def __init__(self, delay: timedelta):
        self.delay = delay

    async def f(self, x, *, session_id: str):
        # blocking work simulation
        await asyncio.sleep(np.abs(self.delay.total_seconds() * (1. + 0.1 * np.random.normal())))
        return x

    async def make_error(self, *, session_id: str):
        # blocking work simulation
        await asyncio.sleep(np.abs(self.delay.total_seconds() * (1. + 0.1 * np.random.normal())))
        raise ValueError("Simulate problem!")

    async def syntax_error(self, session_id: str):
        return None

if __name__ == '__main__':
    ray.init('auto')
    with pytest.raises(ServiceNotFoundError):
        handle = ToyWorker.get_handle()
        handle._f('hello', session_id=f'abc')

    service = ToyWorker(delay=timedelta(seconds=0.5))
    assert service == ToyWorker.fetch()

    handle = service.get_handle()

    try:
        handle._f('hello', session_id=f'abc')
        assert False
    except AttributeError as e:
        assert 'Only public methods' in str(e)

    try:
        ray.get(handle.syntax_error(session_id='abc'))
        assert False
    except SyntaxError as e:
        assert 'keyword-only arg' in str(e)

    async def run1():
        handle = service.get_handle(sync=False)
        loop = get_or_create_event_loop()
        tasks = []
        for i in range(10):
            task = loop.create_task(handle.f('hello', session_id=f'session_{i}'))
            tasks.append(task)
            await asyncio.sleep(np.abs(np.random.laplace(scale=1)))
        obj_refs = await asyncio.gather(*tasks)
        return await asyncio.gather(*obj_refs)

    assert all(res == 'hello' for res in asyncio.run(run1()))

    async def run2():
        handle = service.get_handle(sync=False)
        try:
            await (await handle.make_error(session_id=f'session_abc'))
            assert False
        except ValueError as e:
            assert 'Simulate problem' in str(e)

    asyncio.run(run2())

    # TODO: should also add a test that ensures all workers are dead.

