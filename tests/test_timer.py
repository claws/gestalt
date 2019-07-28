import asyncio
import asynctest
import logging
from gestalt.timer import Timer


class TimersTestCase(asynctest.TestCase):
    async def test_start_stop(self):
        """ check starting and stopping a timer """

        async def user_func(*args, **kwargs):
            pass

        interval = 0.1
        t = Timer(interval, user_func, forever=True)
        await t.start(delay=1.0)
        self.assertTrue(t.started)

        # check starting a started timer has no adverse side effects other
        # than reporting a warning.
        with self.assertLogs("gestalt.timer", level=logging.WARNING) as log:
            await t.start()

        await asyncio.sleep(0.1)

        await t.stop()
        self.assertFalse(t.started)

        # check stopping a stopped timer has no adverse side effects other
        # than reporting a warning.
        with self.assertLogs("gestalt.timer", level=logging.WARNING) as log:
            await t.stop()

    async def test_callback_func(self):
        """ check timer can use a coroutine callback function """

        def user_func_sync(*args, **kwargs):
            pass

        async def user_func(*args, **kwargs):
            pass

        with self.assertRaises(Exception):
            t0 = Timer(0.1, user_func_sync)

        # check passing a coroutine function
        t1 = Timer(0.1, user_func)

    async def test_oneshot(self):
        """ check timer with a single repeat """

        async def user_func(params, **kwargs):
            params["count"] += 1
            if params["count"] > params["repeats"]:
                raise Exception(
                    f"Too many repeats. Expected {params['repeats']} but found {params['count']}"
                )

        count = 0
        interval = 0.1
        repeats = 1
        expected_completion_time = (interval * repeats) + (2 * interval)

        params = dict(count=count, repeats=repeats)
        t = Timer(interval, user_func, params)
        await t.start()
        await asyncio.sleep(expected_completion_time)
        await t.stop()
        self.assertEqual(
            params["count"],
            1,
            f"Expected {params['repeats']} but found {params['count']}",
        )

    async def test_repeating(self):
        """ check timer with specific number of repeats """

        async def user_func(params, **kwargs):
            params["count"] += 1
            if params["count"] > params["repeats"]:
                raise Exception(
                    f"Too many repeats. Expected {params['repeats']} but found {params['count']}"
                )

        count = 0
        interval = 0.1
        repeats = 3
        expected_completion_time = (interval * repeats) + (2 * interval)

        params = dict(count=count, repeats=repeats)
        t = Timer(interval, user_func, params, repeats=repeats)
        await t.start()
        await asyncio.sleep(expected_completion_time)
        await t.stop()
        self.assertEqual(
            params["count"],
            3,
            f"Expected {params['repeats']} but found {params['count']}",
        )

    async def test_forever(self):
        """ check timer that runs forever """

        async def user_func(params, **kwargs):
            params["count"] += 1

        count = 0
        interval = 0.1
        wait_duration = 1.0

        # As the first callback occurs as start + interval, the expected number
        # of calls is one less than the total repeats
        expected_count = (wait_duration / interval) - 1

        params = dict(count=count)
        t = Timer(interval, user_func, params, forever=True)
        await t.start()
        await asyncio.sleep(wait_duration)
        await t.stop()
        self.assertGreaterEqual(
            params["count"],
            expected_count,
            f"Expected {expected_count} but found {params['count']}",
        )
