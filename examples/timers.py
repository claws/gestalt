from gestalt.timer import Timer
from gestalt.runner import run


async def timerFunction(name, **kwargs):
    print(f"Executing {name} timer function")


async def start_demo(t1, t2, t3):
    await t1.start()
    await t2.start()
    await t3.start()


async def stop_demo(t1, t2, t3):
    await t1.stop()
    await t2.stop()
    await t3.stop()


if __name__ == "__main__":

    timer1 = Timer(0.5, timerFunction, "oneshot")
    timer2 = Timer(0.75, timerFunction, "thrice", repeats=3)
    timer3 = Timer(1.0, timerFunction, "forever", forever=True)

    run(start_demo(timer1, timer2, timer3), finalize=stop_demo(timer1, timer2, timer3))
