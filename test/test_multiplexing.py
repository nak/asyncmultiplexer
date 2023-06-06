from random import random

import pytest
import asyncio
from asynciomultiplexer.asynciomultiplexer import AsyncMultiplexedIterator


@pytest.mark.asyncio
async def test_multiplexing_itrator():
    async def aiterator():
        for index in range(10):
            await asyncio.sleep(random()/4.0)
            yield index

    counts = {}
    async with AsyncMultiplexedIterator(aiterator(), aiterator(), aiterator()) as multiplexer:
        async for number in multiplexer:
            counts.setdefault(number, 0)
            counts[number] += 1
            print(number)
            assert number in range(10)
    for i in counts:
        assert counts[i] == 3


@pytest.mark.asyncio
async def test_multiplexing_itrator_premature_exit():
    async def aiterator():
        for index in range(1000):
            await asyncio.sleep(0.2)
            yield index

    counts = {}

    def handle_orphan(number):
        assert number <= 3

    async with AsyncMultiplexedIterator(aiterator(), aiterator(), aiterator(),
                                                 handle_orphan=handle_orphan) as multiplexer:
        async for number in multiplexer:
            counts.setdefault(number, 0)
            counts[number] += 1
            assert number in range(1000)
            if len(counts) == 3:
                break

