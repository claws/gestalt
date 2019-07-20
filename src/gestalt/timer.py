import asyncio
import logging
import inspect
import time
from typing import Callable, Awaitable
from asyncio import AbstractEventLoop

logger = logging.getLogger(__name__)


class Timer(object):
    """
    A timer may fire once, a set number of repeats or periodically. By default
    a timer will fire once. When the timer's wait interval expires it will call
    the user supplied callback function. To configure a timer to repeat for a
    specific number of repeats use the keyword argument repeats=X. To run a
    timer periodically forever use the keyword argument forever=True.
    """

    def __init__(
        self,
        interval: float,
        func: Callable[..., Awaitable],
        *args,
        repeats: int = 1,
        forever: bool = False,
        loop: AbstractEventLoop = None,
        **kwargs,
    ):
        """
        :param interval: a float value representing the amount of time to wait
          between calling the user callback.

        :param func: A callable that will be called upon timer expiry.

        :param args: Remaining positional arguments will be passed directly to
          the callable user function.

        :param repeats: the number of times to call the callback. Defaults to 1.

        :param forever: a boolean flag to instruct the timer to run forever
          (periodically). Defaults to False.

        :param loop: the loop instance to use.

        :param kwargs: Remaining keyword arguments will be passed directly to
          the callable user function.

        """
        if not (inspect.isawaitable(func) or inspect.iscoroutinefunction(func)):
            raise Exception(
                f"func must be an awaitable or a coroutine function, got {func}"
            )
        self._func = func

        self.interval = interval
        self.repeats = repeats
        self.forever = forever
        self.loop = loop or asyncio.get_event_loop()
        self._args = args
        self._kwargs = kwargs

        self._handle = None
        self._task = None
        self._started = False
        self._running = False
        self._cancelled = False

    @property
    def started(self):
        return self._started

    @property
    def running(self):
        return self._running

    @property
    def cancelled(self):
        return self._cancelled

    async def start(self, delay: float = None) -> None:
        """ Start the timer

        If delay is None (the default) then the user function will be called
        after the user specified interval. If delay is any float value then
        the user function will be called after `delay` seconds.
        """
        if self._started:
            return

        self._started = True
        self._running = False
        self._cancelled = False
        self._schedule(delay)

    async def stop(self):
        if not self._started:
            return
        self._cancel()

    def _schedule(self, delay: float = None) -> None:
        """ Schedule a timer event with the event loop """

        if delay is None:
            self._handle = self.loop.call_soon(self._on_timer_expiry)
        else:
            self._handle = self.loop.call_later(delay, self._on_timer_expiry)

    def _cancel(self):
        self._cancelled = True
        if self._handle:
            self._handle.cancel()
        self._handle = None
        if self._task:
            self._task.cancel()
        self._task = None
        self._running = False

    def _on_timer_expiry(self) -> None:
        """
        Execute the user callable.

        Any supplied args and kwargs are passed to the callable.

        If an exception occurs in the callback, the timer will be cancelled.
        """
        if self._cancelled:
            return

        if self.repeats > 0:
            self.repeats -= 1

        if self._running:
            logger.error("Timer function is still running, skipping this call")
        else:
            self.__task = self.loop.create_task(self._runner())

        if self.forever or self.repeats:
            self._schedule(self.interval)
        else:
            self._cancel()

    async def _runner(self):
        self._running = True
        try:
            await self._func(*self._args, **self._kwargs)
        except TypeError as exc:
            logger.exception(
                f"{exc}. Timer func was called with args={self._args} and kwargs={self._kwargs}"
            )
        except Exception:
            logger.exception(f"Timer func {self._func} raised an exception.")
            self._cancel()
        finally:
            self._running = False
