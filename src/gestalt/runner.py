import asyncio
import inspect
import logging
import signal

from asyncio import AbstractEventLoop
from signal import SIGTERM, SIGINT
from typing import Awaitable, Callable, Coroutine, Optional

try:
    # Python3.7
    from asyncio import all_tasks
except ImportError:
    # Python 3.6
    all_tasks = asyncio.Task.all_tasks

logger = logging.getLogger(__name__)


def run(
    func: Optional[Awaitable] = None,
    *,
    finalize: Coroutine = None,
    loop: AbstractEventLoop = None,
):
    """ Start an event loop and run the optional coroutine then
    shutdown the loop when a signal is received.

    This function helps avoid common boilerplate typically needed
    when running asyncio applications.

    :param func: An optional coroutine to run. The coroutine is
      typically the "main" coroutine from which all other work is
      spawned. The event loop will continue to run after the
      supplied coroutine completes. The event loop will still run
      if no coroutine is supplied.

    param finalize: An optional coroutine to run when shutting down.
      This would typically be used to perform any graceful cleanup
      activities such as finalising log files, disconnecting from
      services such as databases, etc.

    :param loop: An optional event loop to run. If not supplied
      the default event loop is used (i.e., whatever
      ``asyncio.get_event_loop()`` returns.

    """
    logger.debug("Application runner starting")

    if not (inspect.isawaitable(func) or inspect.iscoroutinefunction(func)):
        raise Exception(
            f"func must be a coroutine or a coroutine function "
            f"that takes no arguments, got {func}"
        )

    if not (inspect.isawaitable(finalize) or inspect.iscoroutinefunction(finalize)):
        raise Exception(
            f"finalize must be a coroutine or a coroutine function "
            f"that takes no arguments, got {finalize}"
        )

    loop = loop or asyncio.get_event_loop()

    def signal_handler(loop, signame):
        logger.debug(f"Caught {signame}, stopping.")
        loop.call_soon(loop.stop)

    loop.add_signal_handler(SIGINT, signal_handler, loop, "SIGINT")
    loop.add_signal_handler(SIGTERM, signal_handler, loop, "SIGTERM")

    def exception_handler(loop, context):
        logger.critical(f"Caught exception: {context}")
        loop.call_soon(loop.stop)

    loop.set_exception_handler(exception_handler)

    try:
        if inspect.iscoroutinefunction(func):
            func = func()
        loop.create_task(func)

        loop.run_forever()
    finally:
        logger.debug("Application shutdown sequence starting")
        if finalize:
            if inspect.iscoroutinefunction(finalize):
                finalize = finalize()
            loop.run_until_complete(finalize)

        loop.run_until_complete(loop.shutdown_asyncgens())

        # Shutdown any outstanding tasks that are left running
        pending_tasks = all_tasks(loop=loop)
        logger.debug(f"Cancelling {len(pending_tasks)} pending tasks.")
        for task in pending_tasks:
            logger.debug(f"Cancelling task: {task}")
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending_tasks))

        logger.debug("Application shutdown sequence complete")

        loop.close()

        logger.debug("Application runner stopped")
