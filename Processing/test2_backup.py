import asyncio
import inspect
import random
from Data.Utils import pgtimeit
from Processing import pgprocessing

__version__ = "1.5"


def async_to_sync(loop, this_func):
    def func_(*arguments, **kwds):
        return asyncio.run_coroutine_threadsafe(this_func(*arguments, **kwds), loop).result()
    return func_


def sync_code(func):
    print(func.__name__)
    print(inspect.currentframe().f_code.co_name)
    print(inspect.currentframe().f_back.f_code.co_name)
    print(func)
    for i in range(10):
        func(i)


#@pgtimeit.timer(1, 1)
#@processing.pg_asyncio()
async def test1_ct():
    tasks = [mul((random.randint(1, 100)), (random.randint(1, 100)), (random.randint(1, 100))) for _ in range(100)]
    await asyncio.gather(*tasks)


def mul(num1, num2, num3):
    print(locals())
    #print(f"number 1: {num1}")
    #print(f"number 2: {num2}")
    #print(f"number 3: {num3}")
    print(num1 * num2 * num3)



async def async_cb(a):
    print("async callback:", a)


async def main():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, sync_code, async_to_sync(loop, async_cb))

if __name__ == '__main__':
    asyncio.run(test1_ct())

