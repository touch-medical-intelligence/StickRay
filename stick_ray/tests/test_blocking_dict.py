import asyncio
import random
from uuid import uuid4

import pytest

from stick_ray.blocking_dict import BlockingDict


@pytest.fixture
def blocking_dict():
    return BlockingDict()


@pytest.mark.asyncio
async def test_basic_operations(blocking_dict):
    # Test put and peek
    await blocking_dict.put('key', 'value')
    assert await blocking_dict.peek('key') == 'value'

    # Test has
    assert await blocking_dict.has('key')
    assert not await blocking_dict.has('no_key')

    # Test keys
    assert 'key' in blocking_dict.keys()

    # Test size
    assert blocking_dict.size() == 1

    # Test delete
    await blocking_dict.delete('key')
    assert not await blocking_dict.has('key')


@pytest.mark.asyncio
async def test_pop(blocking_dict):
    await blocking_dict.put('key', 'value')

    # Test pop with specified key
    assert await blocking_dict.pop('key') == 'value'
    assert not await blocking_dict.has('key')

    # Test pop with no key (sentenial)
    await blocking_dict.put('key2', 'value2')
    assert await blocking_dict.pop() == 'value2'
    assert not await blocking_dict.has('key2')


@pytest.mark.asyncio
async def test_blocking_peek(blocking_dict):
    async def put_later():
        await asyncio.sleep(0.2)
        await blocking_dict.put('key', 'value')

    asyncio.create_task(put_later())  # Simulate another thread putting a value later
    assert await blocking_dict.peek('key', timeout=0.5) == 'value'


@pytest.mark.asyncio
async def test_blocking_pop(blocking_dict):
    async def put_later():
        await asyncio.sleep(0.2)
        await blocking_dict.put('key', 'value')

    asyncio.create_task(put_later())  # Simulate another thread putting a value later
    assert await blocking_dict.pop('key', timeout=0.5) == 'value'


@pytest.mark.asyncio
async def test_blocking_timeout(blocking_dict):
    with pytest.raises(asyncio.TimeoutError):
        await blocking_dict.peek('key', timeout=0.1)

    with pytest.raises(asyncio.TimeoutError):
        await blocking_dict.pop('key', timeout=0.1)


@pytest.mark.asyncio
async def test_thread_safety(blocking_dict):
    async def put_multiple():
        for i in range(5):
            await blocking_dict.put(f'key{i}', f'value{i}')
            await asyncio.sleep(0.05)  # mimic some random delay

    async def pop_multiple():
        results = []
        for _ in range(5):
            results.append(await blocking_dict.pop(timeout=1))
            await asyncio.sleep(0.03)  # mimic some random delay
        return results

    put_task = asyncio.create_task(put_multiple())
    pop_task = asyncio.create_task(pop_multiple())

    await asyncio.gather(put_task, pop_task)

    results = pop_task.result()
    expected = ['value0', 'value1', 'value2', 'value3', 'value4']
    assert results == expected


@pytest.mark.asyncio
async def test_overwrite_value(blocking_dict):
    await blocking_dict.put('key', 'value1')
    await blocking_dict.put('key', 'value2')
    assert await blocking_dict.peek('key') == 'value2'


@pytest.mark.asyncio
async def test_concurrent_puts(blocking_dict):
    async def put_value(key, value):
        await asyncio.sleep(0.1)  # mimic some delay
        await blocking_dict.put(key, value)

    tasks = [put_value(f'key{i}', f'value{i}') for i in range(10)]
    await asyncio.gather(*tasks)

    for i in range(10):
        assert f'key{i}' in blocking_dict.keys()


@pytest.mark.asyncio
async def test_pop_nonexistent_key(blocking_dict):
    with pytest.raises(asyncio.TimeoutError):
        await blocking_dict.pop('nonexistent_key', timeout=0.1)


@pytest.mark.asyncio
async def test_multiple_consumers(blocking_dict):
    async def consume():
        return await blocking_dict.pop(timeout=1)

    await blocking_dict.put('key', 'value')
    consumers = [consume() for _ in range(5)]

    # Only one consumer should successfully get the value, others should timeout
    results = await asyncio.gather(*consumers, return_exceptions=True)

    assert results.count('value') == 1
    assert sum(1 for r in results if isinstance(r, asyncio.TimeoutError)) == 4


@pytest.mark.asyncio
async def test_delete_nonexistent_key(blocking_dict):
    await blocking_dict.delete('nonexistent_key')  # should not raise any exception


@pytest.mark.asyncio
async def test_pop_order(blocking_dict):
    for i in range(5):
        await blocking_dict.put(f'key{i}', f'value{i}')

    for i in range(5):
        assert await blocking_dict.pop() == f'value{i}'


@pytest.mark.asyncio
async def test_concurrent_deletes(blocking_dict):
    await blocking_dict.put('key', 'value')

    async def delete_key():
        await asyncio.sleep(0.1)
        await blocking_dict.delete('key')

    delete_tasks = [delete_key() for _ in range(5)]
    await asyncio.gather(*delete_tasks)

    assert not await blocking_dict.has('key')


@pytest.mark.asyncio
async def test_high_concurrency_adjusted(blocking_dict):
    NUM_TASKS = 100  # reduce number of tasks
    OPERATIONS_PER_TASK = 1000  # each task will now perform more operations

    async def producer():
        for _ in range(OPERATIONS_PER_TASK):
            key = str(uuid4())
            await blocking_dict.put(key, 'value')

    async def consumer():
        for _ in range(OPERATIONS_PER_TASK):
            await blocking_dict.pop(timeout=1)  # increase the timeout

    tasks = [producer() for _ in range(NUM_TASKS)] + [consumer() for _ in range(NUM_TASKS)]
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_high_concurrency_with_random_delays(blocking_dict):
    NUM_TASKS = 1000

    async def producer():
        for _ in range(NUM_TASKS):
            await asyncio.sleep(random.uniform(0, 0.01))  # random short delay
            key = str(uuid4())
            await blocking_dict.put(key, 'value')

    async def consumer():
        for _ in range(NUM_TASKS):
            await asyncio.sleep(random.uniform(0, 0.01))  # random short delay
            await blocking_dict.pop(timeout=10)

    tasks = [producer() for _ in range(NUM_TASKS)] + [consumer() for _ in range(NUM_TASKS)]
    await asyncio.gather(*tasks)
