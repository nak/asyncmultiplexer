"""
multiplexing utils for parallel (async) tasks
"""

import asyncio
import queue
from typing import AsyncIterator, TypeVar, Generic

__all__ = ["AsyncMultiplexedIterator", "AsyncAdaptorQueue"]
T = TypeVar('T')


class AsyncMultiplexedIterator(Generic[T]):
    """
    Class for multiplexing multiple async iterators in parallel into a single async iterator

    >>> iterators: AsyncIterator[T] = ...
    ... async for device_data in AsyncMultiplexedIterator(*iterators):
    ...     yield device_data
    """

    class Sentinel:
        """
        To mark the end of iteration in the multiplexing queue
        """
        def __init__(self, iterator: AsyncIterator[T]):
            self._iterator = iterator

        @property
        def iterator(self) -> AsyncIterator[T]:
            return self._iterator

    def __init__(self, *iterators: AsyncIterator[T], timeout=0):
        """
        :param iterators: which iterators to iterate over in parallel
        :param timeout: timeout in seconds to wait on next item, or default/zero to wait indefinitely
        """
        self._iterators = list(iterators)
        self._multiplexing_q: asyncio.Queue[T] = asyncio.Queue()
        self._timeout = timeout

    def __aiter__(self) -> "AsyncMultiplexedIterator[T]":
        async def worker(iterator: AsyncIterator[T]):
            try:
                async for item in iterator:
                    await self._multiplexing_q.put(item)
            except BaseException as e:
                await self._multiplexing_q.put(e)
            finally:
                await self._multiplexing_q.put(self.Sentinel(iterator))
        self._tasks = [
            asyncio.create_task(worker(iterator)) for iterator in self._iterators
        ]
        return self

    async def __anext__(self) -> T:
        try:
            while self._iterators:
                if self._timeout > 0:
                    next_item = await asyncio.wait_for(self._multiplexing_q.get(), timeout=self._timeout)
                else:
                    next_item = await self._multiplexing_q.get()
                if isinstance(next_item, BaseException):
                    raise StopAsyncIteration() from next_item
                elif isinstance(next_item, self.Sentinel):
                    self._iterators.remove(next_item.iterator)
                    if not self._iterators:
                        raise StopAsyncIteration()
                    continue
                return next_item
        except Exception:
            for task in [t for t in self._tasks if not t.done()]:
                task.cancel()
            raise


class AsyncAdaptorQueue(Generic[T]):
    """
    A sync-to-async bridge needed for communicating across async loops in multiple threads.
    Since async tasks cannot interact across threads directly, such a queue is needed that
    uses polling-with-sleep on the sync queue to convert to an async method

    In other words, his wraps a synchronous queue  in a separate thread into an asynchronous queue inside
    an asyncio Task.
    """

    def __init__(self, q_size: int):
        self._external_q: queue.Queue[T] = queue.Queue(q_size)
        self._active = True

    async def get(self, polling_interval: int = 1) -> T:
        """
        Get the next item from the queue
        :param polling_interval: optional interval in seconds to poll (defaults to 1 second)
        :return: next item
        """
        while self._active:
            try:
                return self._external_q.get_nowait()
            except queue.Empty:
                await asyncio.sleep(polling_interval)
        raise Exception("Attempt to access closed queue")

    async def put(self, item: T, polling_interval: int = 1) -> None:
        """
        put an item in the queue
        :param item: item to place in queue
        :param polling_interval: optional interval in seconds to poll (defaults to 1 second)
        """
        while self._active:
            try:
                self._external_q.put_nowait(item)
                return
            except queue.Full:
                await asyncio.sleep(polling_interval)
        raise Exception("Attempt to access closed queue")

    def put_nowait(self, item: T) -> None:
        """
        put item in queue non-blocking
        :raises: queue.Full error if queue if full
        """
        self._external_q.put_nowait(item)

    def close(self) -> None:
        """
        Close the queue; no further activity will be allowed
        """
        self._active = False

    def full(self) -> bool:
        """
        :return: if queue is full
        """
        return self._external_q.full()
