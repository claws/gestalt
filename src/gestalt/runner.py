import asyncio
import inspect
import logging
import signal
import sys

from asyncio import AbstractEventLoop
from signal import SIGTERM, SIGINT
from typing import Awaitable, Callable, Coroutine, Optional

if sys.version_info >= (3, 7):
    from asyncio import all_tasks
else:
    # Python 3.6
    all_tasks = asyncio.Task.all_tasks


logger = logging.getLogger(__name__)


def run(
    func: Optional[Awaitable[None]] = None,
    *,
    finalize: Optional[Awaitable[None]] = None,
    loop: AbstractEventLoop = None,
):
    """ Configure the event loop to react to signals and exceptions then
    run the provided coroutine and loop forever.

    Shutdown the event loop when a signal or exception is received or the
    supplied function explicitly requests the loop to stop.

    This function provides some of the common boilerplate typically needed
    when running asyncio applications. It registers signal handlers that
    listen for SIGINT and SIGTERM that will stop the event loop and trigger
    application shutdown actions. It registers a global exception handler
    that will stop the loop and trigger application shutdown actions. This
    helps catch a common problem that occurs in asyncio applications in
    which an exception occurs in a task that was spun off but is not
    reported until the event loop is stopped. This approach allows users
    to be notified about these issues as soon as possible.

    :param func: A coroutine to run before looping forever. This coroutine
      is typically the "main" coroutine from which all other work is spawned.
      The event loop will continue to run after the supplied coroutine
      completes.

    :param finalize: An optional coroutine to run when shutting down. Use this
      to perform any graceful cleanup activities such as finalising log files,
      disconnecting from services such as databases, etc.

    :param loop: An optional event loop to run. If not supplied the default
      event loop is used (i.e., whatever ``asyncio.get_event_loop()`` returns.

    """
    logger.debug("Application runner starting")

    if func:
        if not (inspect.isawaitable(func) or inspect.iscoroutinefunction(func)):
            raise Exception(
                "func must be a coroutine or a coroutine function "
                f"that takes no arguments, got {func}"
            )

    if finalize:
        if not (inspect.isawaitable(finalize) or inspect.iscoroutinefunction(finalize)):
            raise Exception(
                "finalize must be a coroutine or a coroutine function "
                f"that takes no arguments, got {finalize}"
            )

    # Use a supplied loop or the default event loop. If the loop is closed
    # (which can happen in unit tests) then create a new event loop.
    loop = loop or asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()

    def signal_handler(loop, sig):
        logger.info(f"Caught {sig.name}, stopping.")
        loop.call_soon(loop.stop)

    loop.add_signal_handler(SIGINT, signal_handler, loop, SIGINT)
    loop.add_signal_handler(SIGTERM, signal_handler, loop, SIGTERM)

    def exception_handler(loop, context):
        logger.exception(f"Caught exception: {context}")
        loop.call_soon(loop.stop)

    loop.set_exception_handler(exception_handler)

    try:
        if func:
            if inspect.iscoroutinefunction(func):
                func = func()  # type: ignore
            assert func is not None
            loop.create_task(func)
        loop.run_forever()
    finally:
        logger.debug("Application shutdown sequence starting")
        if finalize:
            if inspect.iscoroutinefunction(finalize):
                finalize = finalize()  # type: ignore
            assert finalize is not None
            loop.run_until_complete(finalize)

        # Shutdown any outstanding tasks that are left running
        pending_tasks = all_tasks(loop=loop)
        if pending_tasks:
            logger.debug(f"Cancelling {len(pending_tasks)} pending tasks.")
            for task in pending_tasks:
                logger.debug(f"Cancelling task: {task}")
                task.cancel()
            try:
                loop.run_until_complete(asyncio.gather(*pending_tasks))
            except asyncio.CancelledError:
                pass

        loop.run_until_complete(loop.shutdown_asyncgens())

        logger.debug("Application shutdown sequence complete")

        loop.close()

        logger.debug("Application runner stopped")
