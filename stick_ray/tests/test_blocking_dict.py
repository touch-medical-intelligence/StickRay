import asyncio
import random

import pytest

from stick_ray.blocking_dict import BlockingDict


@pytest.fixture
def blocking_dict():
    return BlockingDict()


@pytest.mark.asyncio
async def test_basic_operations(blocking_dict: BlockingDict):
    # Test put and peek
    await blocking_dict.put('key', 'value')
    assert await blocking_dict.peek('key') == 'value'

    # Test has
    assert blocking_dict.has('key')
    assert not blocking_dict.has('no_key')

    # Test keys
    assert 'key' in blocking_dict.keys()

    # Test size
    assert blocking_dict.size() == 1

    # Test delete
    await blocking_dict.delete('key')
    assert not blocking_dict.has('key')


@pytest.mark.asyncio
async def test_blocking_peek(blocking_dict: BlockingDict):
    async def put_later():
        await asyncio.sleep(0.2)
        await blocking_dict.put('key', 'value')

    asyncio.create_task(put_later())  # Simulate another thread putting a value later
    assert await blocking_dict.peek('key', timeout=0.5) == 'value'


@pytest.mark.asyncio
async def test_blocking_timeout(blocking_dict: BlockingDict):
    with pytest.raises(asyncio.TimeoutError):
        await blocking_dict.peek('key', timeout=0.1)


@pytest.mark.asyncio
async def test_thread_safety(blocking_dict: BlockingDict):
    async def put_multiple():
        for i in range(5):
            await blocking_dict.put(f'key{i}', f'value{i}')
            await asyncio.sleep(0.05)  # mimic some random delay

    async def peek_multiple():
        results = []
        for i in range(5):
            results.append(await blocking_dict.peek(key=f'key{i}', timeout=1))
            await asyncio.sleep(0.03)  # mimic some random delay
        return results

    put_task = asyncio.create_task(put_multiple())
    peek_task = asyncio.create_task(peek_multiple())

    await asyncio.gather(put_task, peek_task)

    results = peek_task.result()
    expected = ['value0', 'value1', 'value2', 'value3', 'value4']
    assert results == expected


@pytest.mark.asyncio
async def test_overwrite_value(blocking_dict: BlockingDict):
    await blocking_dict.put('key', 'value1')
    await blocking_dict.put('key', 'value2')
    assert await blocking_dict.peek('key') == 'value2'


@pytest.mark.asyncio
async def test_concurrent_puts(blocking_dict: BlockingDict):
    async def put_value(key, value):
        await asyncio.sleep(0.1)  # mimic some delay
        await blocking_dict.put(key, value)

    tasks = [put_value(f'key{i}', f'value{i}') for i in range(10)]
    await asyncio.gather(*tasks)

    for i in range(10):
        assert f'key{i}' in blocking_dict.keys()


@pytest.mark.asyncio
async def test_pop_nonexistent_key(blocking_dict: BlockingDict):
    with pytest.raises(asyncio.TimeoutError):
        await blocking_dict.peek('nonexistent_key', timeout=0.1)


@pytest.mark.asyncio
async def test_multiple_consumers(blocking_dict: BlockingDict):
    async def consume():
        return await blocking_dict.peek('key', timeout=1)

    await blocking_dict.put('key', 'value')
    consumers = [consume() for _ in range(5)]

    # Only one consumer should successfully get the value, others should timeout
    results = await asyncio.gather(*consumers, return_exceptions=True)

    assert results.count('value') == 5
    assert sum(1 for r in results if isinstance(r, asyncio.TimeoutError)) == 0


@pytest.mark.asyncio
async def test_delete_nonexistent_key(blocking_dict: BlockingDict):
    await blocking_dict.delete('nonexistent_key')  # should not raise any exception


@pytest.mark.asyncio
async def test_concurrent_deletes(blocking_dict: BlockingDict):
    await blocking_dict.put('key', 'value')

    async def delete_key():
        await asyncio.sleep(0.1)
        await blocking_dict.delete('key')

    delete_tasks = [delete_key() for _ in range(5)]
    await asyncio.gather(*delete_tasks)

    assert not blocking_dict.has('key')


@pytest.mark.asyncio
async def test_high_concurrency_adjusted(blocking_dict: BlockingDict):
    NUM_TASKS = 100  # reduce number of tasks
    OPERATIONS_PER_TASK = 1000  # each task will now perform more operations

    async def producer():
        for i in range(OPERATIONS_PER_TASK):
            key = i
            await blocking_dict.put(key=key, value='value')

    async def consumer():
        for i in range(OPERATIONS_PER_TASK):
            key = i
            await blocking_dict.peek(key=key, timeout=1)  # increase the timeout

    tasks = [producer() for _ in range(NUM_TASKS)] + [consumer() for _ in range(NUM_TASKS)]
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_high_concurrency_with_random_delays(blocking_dict: BlockingDict):
    # This represents an actual stress test of the blocking dict.
    # 1 million concurrent production/consumption events takes about 55 seconds.
    NUM_TASKS = 1000

    async def producer(k):
        for i in range(NUM_TASKS):
            await asyncio.sleep(random.uniform(0, 0.001))  # random short delay
            await blocking_dict.put(f"{k}-{i}", 'value')

    async def consumer(k):
        for i in range(NUM_TASKS):
            await asyncio.sleep(random.uniform(0, 0.001))  # random short delay
            await blocking_dict.peek(f"{k}-{i}", timeout=1)

    tasks = [producer(k) for k in range(NUM_TASKS)] + [consumer(k) for k in range(NUM_TASKS)]
    await asyncio.gather(*tasks)
