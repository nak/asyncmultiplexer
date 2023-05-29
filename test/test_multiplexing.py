import pytest
import asyncio
from asynciomultiplexer import AsyncMultiplexedIterator


@pytest.mark.asyncio
async def test_multiplexing_itrator():
    async def aiterator():
        for index in range(10):
            await asyncio.sleep(0.2)
            yield index

    counts = {}
    async for number in AsyncMultiplexedIterator(aiterator(), aiterator(), aiterator()):
        counts.setdefault(number, 0)
        counts[number] += 1
        assert number in range(10)
    for i in counts:
        assert counts[i] == 3
