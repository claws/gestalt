import asyncio
import logging
import os
import signal
import unittest
import unittest.mock
from gestalt.runner import run


def invalid_func():
    pass


async def valid_func():
    await asyncio.sleep(0.1)
    asyncio.get_event_loop().stop()


async def valid_finalize():
    pass


async def exception_func():
    await asyncio.sleep(0.1)
    raise Exception("Boom")


async def sigint_func():
    await asyncio.sleep(0.1)
    os.kill(os.getpid(), signal.SIGINT)


async def sigterm_func():
    await asyncio.sleep(0.1)
    os.kill(os.getpid(), signal.SIGTERM)


async def spawn_another_task():
    async def a_long_running_task():
        while True:
            await asyncio.sleep(0.05)

    loop = asyncio.get_event_loop()
    loop.create_task(a_long_running_task())

    await asyncio.sleep(0.1)
    loop.stop()


class RunnerTestCase(unittest.TestCase):
    def test_exception_is_raised_if_func_is_not_awaitable(self):
        with self.assertRaises(Exception) as exc:
            run(invalid_func)
        self.assertIn(
            "func must be a coroutine or a coroutine function", str(exc.exception)
        )

    def test_exception_is_raised_if_finalize_is_not_awaitable(self):
        with self.assertRaises(Exception) as exc:
            run(valid_func, finalize=invalid_func)
        self.assertIn(
            "finalize must be a coroutine or a coroutine function", str(exc.exception)
        )

    def test_valid_runner_func(self):
        run(valid_func)
        run(valid_func())
        run(valid_func, finalize=valid_finalize)
        run(valid_func, finalize=valid_finalize())

    def test_handle_exceptions(self):
        with self.assertLogs("gestalt.runner", level=logging.ERROR) as log:
            run(exception_func)
        self.assertIn("Caught exception", log.output[0])

    def test_handle_signals(self):
        with self.assertLogs("gestalt.runner", level=logging.INFO) as log:
            run(sigint_func)
        self.assertIn("Caught SIGINT", log.output[0])

        with self.assertLogs("gestalt.runner", level=logging.INFO) as log:
            run(sigterm_func)
        self.assertIn("Caught SIGTERM", log.output[0])

    def test_pending_tasks_are_cancelled_when_stopping_loop(self):
        with self.assertLogs("gestalt.runner", level=logging.DEBUG) as log:
            run(spawn_another_task)
        self.assertTrue(
            ["Cancelling 1 pending tasks" in log_msg for log_msg in log.output]
        )
