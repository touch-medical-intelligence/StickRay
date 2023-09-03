import asyncio

from stick_ray.stateful_worker import StatefulWorker


def test_stateful_worker():
    class TestStatefulWorker(StatefulWorker):

        async def _close_session(self, session_id: str):
            pass

        async def _create_session(self, session_id: str):
            pass

        async def _start(self):
            pass

        async def _shutdown(self):
            pass

    async def main():
        w = TestStatefulWorker(worker_id='xyz')
        await w.create_session('abc')
        await w.set_session_state('abc', 1)
        assert await w.get_session_state('abc') == 1

    asyncio.run(main())
