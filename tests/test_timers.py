import asyncio
import asynctest
from gestalt.timer import Timer


class TimersTestCase(asynctest.TestCase):
    async def test_oneshot_timer(self):
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

    async def test_repeating_timer(self):
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

    async def test_forever_timer(self):
        """ check timer that runs forever """

        async def user_func(params, **kwargs):
            params["count"] += 1

        count = 0
        interval = 0.1
        wait_duration = 1.0

        # As the first callback occurs as t0 + interval the expected number
        # of calls is one less than the total repeats
        expected_count = (wait_duration / interval) - 1

        params = dict(count=count)
        t = Timer(interval, user_func, params, forever=True)
        await t.start()
        await asyncio.sleep(wait_duration)
        await t.stop()
        self.assertGreater(
            params["count"],
            expected_count,
            f"Expected {expected_count} but found {params['count']}",
        )
