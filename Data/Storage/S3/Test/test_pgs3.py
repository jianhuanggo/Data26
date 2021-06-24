import asyncio
import time
import uuid
import logging
import random
from pprint import pprint
from aiounittest import async_test
import unittest
from Processing import pgprocessing
from Data.Utils import pgtimeit
from Meta import pggenericfunc
from types import FunctionType
from Data.Storage import pgstorage
from Data.StorageFormat import pgstorageformat
from Data.Utils import pgfile
from Data.Utils import pgdirectory
from Data.Storage.S3 import pgs3

from typing import Callable, Iterator

__version__ = "1.5"

DATA = [x for x in range(12)]
TEST_DIR = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk/temp"


#@pgstorageformat.pgstorageformat(storage_format='excel')
@pgstorage.pgstorage(storage_type='s3')
def blocks(client_num: int, parameter, storage=None, storage_format=None):
    log = logging.getLogger('client({})'.format(client_num))
    bucket_name = parameter["bucket_name"]
    object_key = f"file_{str(uuid.uuid4().hex[:8])}"
    storage.set_param(storage_parameter={'s3_bucket_location': bucket_name,
                                         'object_key': object_key,
                                         'mode': parameter['mode']
                                        })
    _data = []
    while DATA:
        print(len(DATA))
        #log.info(f"clien_num: {client_num}")
        #log.info(f"parameter: {parameter}")
        log.info('running')
        data = DATA.pop()
        if data is None:
            break
        result = str(data ** 2)
        _data.append(f"client {client_num}, the result of {data} square is {result}\n")

        log.info('done')
        time.sleep(random.randint(0, 5))

        #print(storage)
        #print(storage_format)
        storage.save(f"client {client_num}, the result of {data} square is {result}\n")

        #log.info(f"client {client_num}, the result of {data} square is {result}\n")


    #storage.save(''.join(output))
    storage.save(''.join(_data))
    return True


@pgstorage.pgstorage(storage_type='s3')
async def run_blocking_tasks(num_client: int, parameter: dict, storage=None, executor=None) -> list:
    random_string = str(uuid.uuid4().hex[:8])
    #storage.create_bucket(f"test{random_string}")
    variable_list = []
    for i in range(num_client):
        variable_list.append((i, parameter))

    result = await executor.run(blocks, *variable_list)
    return result


@pgstorage.pgstorage(storage_type='s3')
def get_count(bucket, storage=None, storageformat=None):
    result = []
    storage.set_param(storage_parameter={'mode': "direct"})
    storage.load("s3://testba8a4bda")
    pprint(storage._data)

    for _data in storage._data:
        for lines in _data['StreamingBody'].read().decode('utf-8').split('\n'):
            result.append(lines)

    #for filename in pgfile.get_all_file_in_dir(TEST_DIR):
    #    with open(pgdirectory.add_splash_2_dir(TEST_DIR) + filename, 'r') as file:
    #        for line in file.readlines():
    #            result.append(int(line.split()[-1]))
    return [int(line.split()[-1]) for line in result if line]


@pgstorage.pgstorage(storage_type='s3')
def get_count2(bucket, storage=None, storageformat=None):
    result = []
    local_directory = '/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/S3/Test/temp'
    storage.set_param(storage_parameter={'mode': "file",
                                         'directory': '/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/S3/Test/temp'})
    storage.load("s3://testba8a4bda")

    for filename in pgdirectory.files_in_dir(local_directory):
        with open(f"{pgdirectory.add_splash_2_dir(local_directory)}{filename}", 'r') as file:
            result.append(file.read())

    #[word for sentence in text for word in sentence]
    return set([int(x.split()[-1]) for a in [line.split('\n') for line in result if line if line] for x in a if x])


class MyTest(unittest.TestCase):
    """
    @async_test
    async def test_decorator_asyncio_multithreading(self):
        #pgdirectory.remove_all_file_in_dir(TEST_DIR)
        s3 = pgs3.PGS3()

        for key in s3.s3_client.list_objects(Bucket='testba8a4bda')['Contents']:
            #print(key['Key'])
            s3.delete_s3_obj(bucket_name='testba8a4bda', key=key['Key'])
        input_data = DATA.copy()
        total_client_number = 6
        io_mt = processing.pg_asyncio()(processing.pg_multithreading(num_workers=total_client_number)(run_blocking_tasks))
        parameter = {'test': 10,
                     'mode': 'direct',
                     'bucket_name': 'testba8a4bda'}
        ret = await io_mt(num_client=total_client_number, parameter=parameter)
        self.assertEqual(sorted(get_count(bucket='testba8a4bda')), sorted([x ** 2 for x in input_data]))

    """
    @async_test
    async def test_decorator_multiprocessing_asyncio(self):
        pgdirectory.remove_all_file_in_dir(
            "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/S3/Test/temp")
        s3 = pgs3.PGS3()
        for key in s3.s3_client.list_objects(Bucket='testba8a4bda')['Contents']:
            s3.delete_s3_obj(bucket_name='testba8a4bda', key=key['Key'])
        input_data = DATA.copy()
        total_client_number = 6
        mp_io = pgprocessing.pg_multiprocessing(num_workers=total_client_number)(pgprocessing.pg_asyncio()(run_blocking_tasks))
        parameter = {'mode': 'direct',
                     'bucket_name': 'testba8a4bda',
                     }

        ret = await mp_io(num_client=total_client_number, parameter=parameter)

        self.assertEqual(sorted(get_count2(bucket='testba8a4bda')), sorted([x ** 2 for x in input_data]))


if __name__ == '__main__':
    unittest.main()
    #func_test("mp_io")
    #func_test("mt_mp_io")
