import sys
import asyncio
import aiohttp
from pprint import pprint
import random
from Data.Utils import pgtimeit
from Processing import pgprocessing
from Processing.Multithreading import pgmultithreading
from Processing.Multiprocessing import pgmultiprocessing

__version__ = "1.5"


async def _mul(num1, num2, num3):
    print(locals())
    print(num1 * num2 * num3)

URL = 'https://httpbin.org/uuid'


async def _fetch(session, url):
    async with session.get(url) as response:
        json_response = await response.json()
        print(json_response['uuid'])


@pgprocessing.pg_asyncio()
async def fetch(executor=None):
    async with aiohttp.ClientSession() as session:
        #tasks = [fetch(session, URL) for _ in range(100)]
        #await asyncio.gather(*tasks)
        tasks = [(session, URL) for _ in range(100)]
        await executor.run(_fetch, *tasks)


@pgprocessing.pg_asyncio()
async def mul(executor=None):
    variable_list = [((random.randint(1, 100)), (random.randint(1, 100)), (random.randint(1, 100))) for _ in range(100)]
    await executor.run(_mul, *variable_list)


@pgtimeit.timer(1, 1)
def func():
    #asyncio.run(mul())
    asyncio.run(fetch())

#def test10(executor, a):
@pgprocessing.pg_asyncio()
@pgprocessing.pg_multithreading(num_workers=6)
def test10(executor=None):
    print(type(executor))
    print(f"num_workers :{executor._num_workers}")
    print(f"pool_executor: {executor._pool_executor}")
    exec = "executor"
    #print(locals())
    #print(isinstance(locals()[exec], (pgmultithreading.PGMultiThreading, pgmultiprocessing.PGMultiProcessing)))


def tt(func):
    print(func.__code__.co_varnames)
    print(func.__code__.co_names)
    print(func.__dict__)

    #getattr(, 'age')
    #pprint(getattr(func, 'exec'))


if __name__ == '__main__':
    #func()
    #tt(test10)
    test10()



