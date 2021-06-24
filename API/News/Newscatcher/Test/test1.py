import logging
from aiounittest import async_test
import unittest
from Processing import pgprocessing
from API.News.Newscatcher import pgnewscatcher
from API.News import pgnews
from typing import Callable, Iterator

__version__ = "1.5"


def worker(client_num: int, parameter, data, storage=None, storage_format=None):
    log = logging.getLogger('client({})'.format(client_num))
    test_urls = data.get_urls()
    print(test_urls)
    processed_list = []

    for _data_index, _data_element in enumerate(test_urls):
        if (_data_index % parameter['total_client_number']) == client_num:
            data.process(_data_element)
            processed_list.append(f"{client_num}: {_data_element}")
            log.info(f"processed url {_data_element}")
    return processed_list


async def run_tasks(num_client: int, parameter: dict, data: object, executor=None) -> list:

    variable_list = []
    for i in range(num_client):
        variable_list.append((i, parameter, data))

    result = await executor.run(worker, *variable_list)
    return result


def check_result(data, storage=None, storage_format=None):
    #print(data.data)
    return len(data.data)


class MyTest(unittest.TestCase):

    @async_test
    async def test_1(self):
        test = pgnewscatcher.PGNewsCatcher()
        print(f"country: {test.country}")
        print(f"topic: {test.topic}")
        print(f"language: {test.language}")
        urls = test.get_urls()
        test._process(urls)
        self.assertGreaterEqual(len(test._data), 150)

    @async_test
    async def test_2(self):
        testExt = pgnewscatcher.PGNewsCatcherExt()
        print(f"country: {testExt.country}")
        print(f"topic: {testExt.topic}")
        print(f"language: {testExt.language}")
        print(testExt.get_all_urls())
        self.assertEqual(testExt.get_all_urls()[0], "facebook.com")


    @async_test
    @pgnews.api_news(object_type="newscatcher", object_name="mynewscatcher", subscription_level=1)
    async def test_3_asyncio_multithreading(self, _pg_action=None):
        test = _pg_action.newscatcher.mynewscatcher
        print(f"country: {test.country}")
        print(f"topic: {test.topic}")
        print(f"language: {test.language}")

        total_client_number = 6
        io_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multithreading(num_workers=total_client_number)(run_tasks))
        ret = await io_mt(num_client=total_client_number, parameter={'total_client_number': total_client_number}, data=test)
        self.assertGreaterEqual(check_result(data=test), 150)


if __name__ == '__main__':
    unittest.main()


