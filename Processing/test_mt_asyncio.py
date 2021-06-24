import asyncio
from concurrent import futures
import logging
import unittest
import sys
import time
from Processing import pgprocessing
from Data.Utils import pgtimeit

__version__ = "1.5"


def blocks(n):
    log = logging.getLogger('blocks({})'.format(n))
    log.info('running')
    time.sleep(0.1)
    log.info('done')
    return n ** 2


#@processing.pg_asyncio(logger=logging.getLogger('run_blocking_tasks'),
#                        pool_executor=futures.ThreadPoolExecutor(max_workers=3))
#@processing.pg_multithreading(num_workers=6)
@pgprocessing.pg_multithreading(num_workers=6)
@pgprocessing.pg_asyncio(logger=logging.getLogger('run_blocking_tasks'))
async def run_blocking_tasks(executor=None):
    executor._logger.info('starting')
    executor._logger.info('creating executor tasks')

    print(f"num of worker: {executor._num_workers}")
    print(f"pool executor: {executor._pool_executor}")

    variable_list = []
    for i in range(6):
        variable_list.append(i)

    await executor.run_sync(blocks, *variable_list)


@pgprocessing.pg_asyncio(logger=None)
async def ok(executor=None):
    print("test")


@pgtimeit.timer(1, 1)
def func():
    #asyncio.run(mul())
    asyncio.run(ok())


@pgtimeit.timer(1, 1)
def func_test():
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(
            run_blocking_tasks()
        )
    finally:
        event_loop.close()


if __name__ == '__main__':

    # Configure logging to show the name of the thread
    # where the log message originates.

    logging.basicConfig(
        level=logging.INFO,
        format='%(threadName)10s %(name)18s: %(message)s',
        stream=sys.stderr,
    )
    func_test()


    """
    class TestSum(unittest.TestCase):

        async def test_multiple_decorators(self):
            logging.basicConfig(
                level=logging.INFO,
                format='%(threadName)10s %(name)18s: %(message)s',
                stream=sys.stderr,
            )
            # processing.pg_asyncio(logger=logging.getLogger('run_blocking_tasks'))(run_blocking_tasks)
            result = self.assertEqual(await func_test(), await func_test())


        def test_sum_tuple(self):
            self.assertEqual(sum((1, 2, 2)), 6, "Should be 6")

        def test_bad_type(self):
            data = "banana"
            with self.assertRaises(TypeError):
                result = sum(data)

    if __name__ == '__main__':
        
        unittest.main()
        
        """