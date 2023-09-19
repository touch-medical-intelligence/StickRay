import asyncio
from datetime import timedelta
from time import sleep

import numpy as np
import ray

from stick_ray.routed_services import routed_service, NoRoutedServiceFound
from stick_ray.stateful_worker import StatefulWorker
from stick_ray.utils import get_or_create_event_loop


@routed_service(expiry_period=timedelta(seconds=10), max_concurrent_sessions=2)
class ToyWorker(StatefulWorker):
    """
    A toy worker that simulates some blocking work.
    """
    def __init__(self, delay: timedelta, **kwargs):
        StatefulWorker.__init__(self, **kwargs)
        self.delay = delay

    async def _close_session(self, session_id: str):
        pass

    async def _create_session(self, session_id: str):
        pass

    async def _start(self):
        pass

    async def _shutdown(self):
        pass

    async def f(self, x, *, session_id: str):
        # blocking work simulation
        sleep(np.abs(self.delay.total_seconds() * (1. + 0.1 * np.random.normal())))
        return x

    async def make_error(self, *, session_id: str):
        # blocking work simulation
        sleep(np.abs(self.delay.total_seconds() * (1. + 0.1 * np.random.normal())))
        raise ValueError("Simulate problem!")

    async def syntax_error(self, session_id: str):
        return None


def test_run():
    ray.init(address='auto', ignore_reinit_error=True)
    try:
        ToyWorker.fetch()
        assert False
    except NoRoutedServiceFound:
        assert True

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

    async def test_1():
        handle = service.get_handle(sync=False)
        loop = get_or_create_event_loop()
        tasks = []
        for i in range(10):
            task = loop.create_task(handle.f('hello', session_id=f'session_{i}'))
            tasks.append(task)
            await asyncio.sleep(np.abs(np.random.laplace(scale=1)))
        obj_refs = await asyncio.gather(*tasks)
        return await asyncio.gather(*obj_refs)

    assert all(res == 'hello' for res in asyncio.run(test_1()))

    async def test_2():
        handle = service.get_handle(sync=False)
        try:
            await (await handle.make_error(session_id=f'session_abc'))
            assert False
        except ValueError as e:
            assert 'Simulate problem' in str(e)

    asyncio.run(test_2())

    # TODO: should also add a test that ensures all workers are dead.
