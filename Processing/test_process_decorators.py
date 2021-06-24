import asyncio
import logging
from aiounittest import async_test
import unittest
import time
from Processing import pgprocessing
from Data.Utils import pgtimeit
from Meta import pggenericfunc
from types import FunctionType


__version__ = "1.5"


def blocks(n):
    log = logging.getLogger('blocks({})'.format(n))
    log.info('running')
    time.sleep(0.1)
    log.info('done')
    return n ** 2


# @processing.pg_asyncio(logger=logging.getLogger('run_blocking_tasks'),
#                        pool_executor=futures.ThreadPoolExecutor(max_workers=3))


async def run_blocking_tasks(executor=None):

    variable_list = []
    for i in range(6):
        variable_list.append(i)

    result = await executor.run(blocks, *variable_list)
    return result

io_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multithreading(num_workers=6)(run_blocking_tasks))
mt_io = pgprocessing.pg_multithreading(num_workers=6)(pgprocessing.pg_asyncio()(run_blocking_tasks))
io_mp = pgprocessing.pg_asyncio()(pgprocessing.pg_multiprocessing(num_workers=6)(run_blocking_tasks))
mp_io = pgprocessing.pg_multiprocessing(num_workers=6)(pgprocessing.pg_asyncio()(run_blocking_tasks))
mt_mp_io = pgprocessing.pg_multithreading(num_workers=1)(pgprocessing.pg_multiprocessing(num_workers=6)(pgprocessing.pg_asyncio()(run_blocking_tasks)))
io_mp_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multiprocessing()(pgprocessing.pg_multithreading(num_workers=1)(run_blocking_tasks)))


@pgtimeit.timer(1, 1)
def func_test(func_name: str):
    func_map = {'io_mt': io_mt,
                'mt_io': mt_io,
                'mp_io': mp_io,
                'io_mp': io_mp,
                'mt_mp_io': mt_mp_io,
                'io_mp_mt': io_mp_mt,
                'others': pggenericfunc.notimplemented

    }
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(
            #run_blocking_tasks()
            func_map.get(func_name, "others")()

        )
    finally:
        event_loop.close()


class MyTest(unittest.TestCase):

    @async_test
    async def test_decorator_asyncio_multithreading(self):
        ret = await io_mt()
        sort_ret = sorted(ret)
        self.assertEqual(sort_ret, sorted([4, 0, 25, 9, 1, 16]))

    @async_test
    async def test_decorator_multithreading_asyncio(self):
        ret = await mt_io()
        sort_ret = sorted(ret)
        self.assertEqual(sort_ret, sorted([4, 0, 25, 9, 1, 16]))

    @async_test
    async def test_decorator_multiprocessing_asyncio(self):
        ret = await mp_io()
        sort_ret = sorted(ret)
        self.assertEqual(sort_ret, sorted([4, 0, 25, 9, 1, 16]))

    @async_test
    async def test_decorator_asyncio_multiprocessing(self):
        ret = await io_mp()
        sort_ret = sorted(ret)
        self.assertEqual(sort_ret, sorted([4, 0, 25, 9, 1, 16]))

    @async_test
    async def test_decorator_asyncio_multiprocessing_multithreading(self):
        ret = await io_mp_mt()
        sort_ret = sorted(ret)
        self.assertEqual(sort_ret, sorted([4, 0, 25, 9, 1, 16]))

    @async_test
    async def test_decorator_multithreading_multiprocessing_asyncio(self):
        ret = await io_mp_mt()
        sort_ret = sorted(ret)
        self.assertEqual(sort_ret, sorted([4, 0, 25, 9, 1, 16]))


if __name__ == '__main__':
    unittest.main()
    #func_test("mp_io")
    #func_test("mt_mp_io")
