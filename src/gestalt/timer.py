import asyncio
import logging
import inspect
from typing import Awaitable, Callable, Optional
from asyncio import AbstractEventLoop

logger = logging.getLogger(__name__)


class Timer:
    """
    The Timer object aims to simplify creating periodic calls to a function.

    A Timer can be configured to call its callback once, a specific number of
    repeats or forever. By default a timer is configured to fire once.

    When the timer's wait interval expires it will call the user supplied
    callback function.

    To configure a timer to repeat for a specific number of repeats use the
    keyword argument ``repeats=X``.

    To configure a timer to call the callback function forever, use the
    keyword argument ``forever=True``.
    """

    def __init__(
        self,
        interval: float,
        coroutine_func: Callable[..., Awaitable],
        *args,
        repeats: int = 1,
        forever: bool = False,
        loop: AbstractEventLoop = None,
        **kwargs,
    ):
        """
        :param interval: a float value representing the amount of time to wait
          between calling the user callback.

        :param coroutine_func: A coroutine function that will be called upon
          timer expiry. The argument must be a coroutine function that takes
          positional arguments and keyword arguments such as this:

          .. code-block:: python

                async def my_timer_callback(*args, **kwargs):
                    pass

        :param args: Remaining positional arguments will be passed directly to
          the callable user function.

        :param repeats: a specific number of times to call the callback. This
          argument is used to limit the number of times the timer will call the
          callback. Defaults to 1 so that the default operation of a timer is
          a single shot timer.

        :param forever: a boolean flag to instruct the timer to run forever
          (periodically). Defaults to False.

        :param loop: a specific loop instance to use. If not specified the
          default loop instance is used.

        :param kwargs: Remaining keyword arguments will be passed directly to
          the callable user function.

        """
        if not inspect.iscoroutinefunction(coroutine_func):
            raise Exception(f"func must be a coroutine function, got {coroutine_func}")

        self.interval = interval
        self.repeats = repeats
        self.forever = forever
        self.loop = loop or asyncio.get_event_loop()

        self._func = coroutine_func
        self._args = args
        self._kwargs = kwargs

        self._handle = None  # type: Optional[asyncio.Handle]
        self._task = None  # type: Optional[asyncio.Task]
        self._started = False
        self._running = False
        self._cancelled = False

    @property
    def started(self):
        """ Return True if the timer has been started """
        return self._started

    @property
    def running(self):
        """ Return True if the timer callback function is currently running """
        return self._running

    @property
    def cancelled(self):
        """ Return True if the timer has been cancelled.

        A timer may be cancelled if it has executed all of its repeats or
        if an exception occurred in the user supplied callback function.
        """
        return self._cancelled

    async def start(self, delay: float = None) -> None:
        """ Start the timer

        :param delay: An optional value representing an initial delay, in
          seconds, to wait before calling the callback function the first
          time. Subsequent calls will be made at the interval specified at
          initialization. If not specified then the default value used will
          be the interval specified at initialization.
        """
        if self._started:
            logger.warning("Timer is already started")
            return

        self._started = True
        self._running = False
        self._cancelled = False
        self._schedule(delay or self.interval)

    async def stop(self):
        """ Stop the timer """
        if not self._started:
            logger.warning("Timer is already stopped")
            return
        self._cancel()
        self._started = False

    def _schedule(self, delay: float = None) -> None:
        """ Schedule a timer event with the event loop.

        :param delay: A value representing a delay before calling the callback
          function. Default value is None which will result in the callback
          function being called immediately.
        """
        if delay is None:
            self._handle = self.loop.call_soon(self._on_timer_expiry)
        else:
            self._handle = self.loop.call_later(delay, self._on_timer_expiry)

    def _cancel(self, *args):
        """ Cancel the timer.

        This method may be called directly or from a Task's done_callback. In
        this last use case it needs to accept an argument (the task result),
        which is why this method takes an optional 'args' parameter.
        """
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
        Called upon expiry of the timer interval to call the user callback.
        """
        if self._cancelled:
            return

        if self.repeats > 0:
            self.repeats -= 1

        if self._running:
            logger.error("Timer function is still running, skipping this call")
        else:
            self._task = self.loop.create_task(self._runner())

        if self.forever or self.repeats:
            self._schedule(self.interval)
        else:
            # This block is executed when a repeating timer has been called
            # for the last time. Have the task cancel the timer when it
            # completes.
            assert isinstance(self._task, asyncio.Task)  # satisfy mypy
            self._task.add_done_callback(self._cancel)

    async def _runner(self):
        """ Execute the user callback function.

        If an exception occurs in the user callback, the timer will be cancelled.
        """
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
