import time
import logging
import random
from aiounittest import async_test
import unittest
from Processing import pgprocessing
from Data.Storage import pgstorage
from Data.StorageFormat import pgstorageformat

__version__ = "1.5"

DATA = [x for x in range(30)]
TEST_DIR = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk/temp"


@pgstorageformat.pgstorageformat(storage_format='excel')
@pgstorage.pgstorage(storage_type='localdisk')
def blocks(client_num: int, parameter, storage=None, storage_format=None):

    log = logging.getLogger('client({})'.format(client_num))
    storage.set_param(storage_parameter={'directory': TEST_DIR,
                                        'filename': f"client{client_num}.xlsx",
                                        #'write_mode': 'a'
                                        })
    storage_format.set_param(storage_parameter={'header': True})

    output = []
    while DATA:
        log.info('running')
        data = DATA.pop()
        if data is None:
            break
        log.info('done')
        time.sleep(random.randint(0, 5))
        result = str(data ** 2)
        output.append(f"{client_num}, {data}, {result}\n")
        log.info(f"client {client_num}, the result of {data} square is {result}\n")

    print(output)
    storage.save(''.join(output))
    return True


async def run_blocking_tasks(num_client: int, parameter: dict, executor=None) ->list:
    variable_list = []
    for i in range(num_client):
        variable_list.append((i, parameter))

    result = await executor.run(blocks, *variable_list)
    return result


@pgstorageformat.pgstorageformat(storage_format='excel')
@pgstorage.pgstorage(storage_type='localdisk')
def get_count(storage=None, storage_format=None):
    #storage_format.set_param(storage_parameter={'header': True})
    storage.load(TEST_DIR)
    return storage_format.data.iloc[:, -1].values.tolist()


class MyTest(unittest.TestCase):

    @async_test
    async def test_decorator_asyncio_multithreading(self):
        input_data = DATA.copy()
        total_client_number = 6
        io_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multithreading(num_workers=total_client_number)(run_blocking_tasks))
        parameter = {'test': 10}
        ret = await io_mt(num_client=total_client_number, parameter=parameter)
        self.assertEqual(sorted(get_count()), sorted([x ** 2 for x in input_data]))


if __name__ == '__main__':
    unittest.main()

