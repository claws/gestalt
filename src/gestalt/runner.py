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
    """ Start an event loop and run an optional coroutine.

    Shutdown the loop when a signal is received or the supplied function
    explicitly requests the loop to stop.

    This function provides some of the common boilerplate typically needed
    when running asyncio applications. It registers signal handlers that
    listen for SIGINT and SIGTERM that will stop the event loop and trigger
    application shutdown actions. It registers a global exception handler
    that will stop the loop and trigger application shutdown actions. This
    helps catch a common problem that occurs in asyncio applications in
    which an exception occurs in a task that was spun off but is not
    reported until the event loop is stopped. This approach allows users
    to be notified about these issues as soon as possible.

    :param func: An optional coroutine to run. The coroutine is typically
      the "main" coroutine from which all other work is spawned. The event
      loop will continue to run after the supplied coroutine completes.
      The event loop will still run if no coroutine is supplied.

    :param finalize: An optional coroutine to run when shutting down. Use this
      to perform any graceful cleanup activities such as finalising log files,
      disconnecting from services such as databases, etc.

    :param loop: An optional event loop to run. If not supplied the default
      event loop is used (i.e., whatever ``asyncio.get_event_loop()`` returns.

    """
    logger.debug("Application runner starting")

    if not (inspect.isawaitable(func) or inspect.iscoroutinefunction(func)):
        raise Exception(
            "func must be a coroutine or a coroutine function "
            f"that takes no arguments, got {func}"
        )

    if not (inspect.isawaitable(finalize) or inspect.iscoroutinefunction(finalize)):
        raise Exception(
            "finalize must be a coroutine or a coroutine function "
            f"that takes no arguments, got {finalize}"
        )

    loop = loop or asyncio.get_event_loop()

    def signal_handler(loop, sig):
        logger.debug(f"Caught {sig.name}, stopping.")
        loop.call_soon(loop.stop)

    loop.add_signal_handler(SIGINT, signal_handler, loop, SIGINT)
    loop.add_signal_handler(SIGTERM, signal_handler, loop, SIGTERM)

    def exception_handler(loop, context):
        logger.exception(f"Caught exception: {context}")
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
        if pending_tasks:
            logger.debug(f"Cancelling {len(pending_tasks)} pending tasks.")
            for task in pending_tasks:
                logger.debug(f"Cancelling task: {task}")
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending_tasks))

        logger.debug("Application shutdown sequence complete")

        loop.close()

        logger.debug("Application runner stopped")
