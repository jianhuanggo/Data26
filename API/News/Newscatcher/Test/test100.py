import unittest
from aiounittest import async_test
from API.News import pgnews
from Processing import pgprocessing


class MyTest(unittest.TestCase):

    @async_test
    async def test_3(self, _pg_action=None):
        _process_object_map = {'_pg_trigger': {},
                               '_pg_action': {'newscatcher': pgnews.api_news},
                               '_pg_storageformat': {},
                               '_pg_storage': {},
                               }
        _parameters = {}
        _news_proc = pgprocessing.pg_simple_worker
        total_client_number = 6
        for _element, _detail in _process_object_map.items():
            if _detail:
                for _object_type, _object in _detail.items():
                    _news_proc = _object(object_type=_object_type, variable_name=_element, subscription_level=999)(_news_proc)
                    _parameters[_element] = _object_type



        #news_proc = pgnews.api_news(object_type="newscatcher", variable_name='_pg_action', subscription_level=999)(
        #    pgprocessing.pg_simple_worker)

        news_run_tasks = pgprocessing.pg_asyncio()(pgprocessing.pg_multithreading(num_workers=total_client_number)(pgprocessing.pg_run_tasks))
        ret = await news_run_tasks(func=_news_proc, num_client=total_client_number, parameter=_parameters)
        self.assertGreaterEqual(150, 150)


if __name__ == '__main__':
    unittest.main()


"""
pg_simple_worker(client_num: int, tasks=None, parameters=None, 
                  _pg_trigger=None, _pg_action=None, _pg_storageformat=None,
                  _pg_storage=None, logger=None, *args, **kwargs):
    async def pg_run_tasks(num_client: int, parameter: dict = None, executor=None) -> list:
"""