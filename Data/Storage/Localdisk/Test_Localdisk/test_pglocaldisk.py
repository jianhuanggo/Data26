import asyncio
import logging
import random
from aiounittest import async_test
import unittest
import time
from Processing import pgprocessing
from Data.Utils import pgtimeit
from Meta import pggenericfunc
from types import FunctionType
from Data.Storage import pgstorage
from Data.StorageFormat import pgstorageformat
from Data.Utils import pgfile
from Data.Utils import pgdirectory
import time
from typing import Callable, Iterator

__version__ = "1.5"

DATA = [x for x in range(30)]
TEST_DIR = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk/temp"


#@pgstorageformat.pgstorageformat(storage_format='excel')
@pgstorage.pgstorage(storage_type='localdisk')
def blocks(client_num: int, parameter, storage=None, storage_format=None):

    log = logging.getLogger('client({})'.format(client_num))
    storage.set_param(storage_parameter={'directory': TEST_DIR,
                                         'filename': f"client{client_num}.xlsx",
                                        #'write_mode': 'a'
                                        })

    while DATA:
        #log.info(f"clien_num: {client_num}")
        #log.info(f"parameter: {parameter}")
        log.info('running')
        data = DATA.pop()
        if data is None:
            break
        log.info('done')
        time.sleep(random.randint(0, 5))
        result = str(data ** 2)
        #print(storage)
        #print(storage_format)
        storage.save(f"client {client_num}, the result of {data} square is {result}\n")
        #print(data)
        log.info(f"client {client_num}, the result of {data} square is {result}\n")

    #print(f"output: {output}")
    #storage.save(''.join(output))
    return True



#Iterator
#@pgtimeit.timer(1, 1)
async def run_blocking_tasks(num_client: int, parameter: dict, executor=None) ->list:
    variable_list = []
    for i in range(num_client):
        variable_list.append((i, parameter))

    result = await executor.run(blocks, *variable_list)
    return result


def get_count(storage=None, storageformat=None):
    result = []

    for filename in pgfile.get_all_file_in_dir(TEST_DIR):
        with open(pgdirectory.add_splash_2_dir(TEST_DIR) + filename, 'r') as file:
            for line in file.readlines():
                result.append(int(line.split()[-1]))
    return result


class MyTest(unittest.TestCase):

    @async_test
    async def test_decorator_asyncio_multithreading(self):
        pgdirectory.remove_all_file_in_dir(TEST_DIR)
        input_data = DATA.copy()
        total_client_number = 6
        io_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multithreading(num_workers=total_client_number)(run_blocking_tasks))
        parameter = {'test': 10}
        ret = await io_mt(num_client=total_client_number, parameter=parameter)
        self.assertEqual(sorted(get_count()), sorted([x ** 2 for x in input_data]))

    """
    @async_test
    async def test_decorator_multiprocessing_asyncio(self):

        #pgdirectory.remove_all_file_in_dir(TEST_DIR)
        input_data = DATA.copy()
        total_client_number = 6
        mp_io = processing.pg_multiprocessing(num_workers=total_client_number)(processing.pg_asyncio()(run_blocking_tasks))
        parameter = {'test': 10}
        ret = await mp_io(num_client=total_client_number, parameter=parameter)
        result = get_count()
        print(result)
        self.assertEqual(sorted(result), sorted([x ** 2 for x in input_data]))

    """


if __name__ == '__main__':
    unittest.main()
    #func_test("mp_io")
    #func_test("mt_mp_io")
