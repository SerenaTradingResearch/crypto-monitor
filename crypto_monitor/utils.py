import asyncio
import signal
import sys
from functools import wraps
from typing import List

from PIL import Image


def retry(sleep=60):
    def decorator(func):
        @wraps(func)
        async def func2(*args, **kwargs):
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    print(f"Retrying after {e}")
                    await asyncio.sleep(sleep)

        return func2

    return decorator


async def gather_n_cancel(*tasks):
    tasks = [asyncio.create_task(x) for x in tasks]
    try:
        return await asyncio.gather(*tasks)
    except Exception as e:
        [x.cancel() for x in tasks]
        raise e


def chunk(lst, n):
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def save_gif(frames: List[Image.Image], id, fps=5):
    frames[0].save(
        f"{id}.gif",
        format="GIF",
        append_images=frames[1:],
        save_all=True,
        duration=int(1000 / fps),
        loop=0,
        optimize=True,
    )


class SafeExit:
    signal = 0

    def __init__(s):
        def handler(sig, frame):
            s.signal = 1

        for SIG in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(SIG, handler)

    def check(s):
        if s.signal:
            sys.exit(0)


safe_exit = SafeExit()
