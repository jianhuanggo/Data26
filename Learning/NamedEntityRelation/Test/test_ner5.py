import time
import asyncio
import inspect
import logging
from pprint import pprint
from API.SocialMedia import pgsocialmedia
from Learning import pglearning
from Processing import pgprocessing
from Meta import pggenericfunc


### scrapy crawl pgsiders -o output.json


def pg_worker_v2(client_num: int, parameters=None, tasks=None,
                 _pg_trigger=None, _pg_action=None, _pg_storageformat=None,
                 _pg_storage=None, _pg_db=None, _logger=None, *args, **kwargs):
    _logger = logging.getLogger('client({})'.format(client_num))

    try:
        ### check variables
        _var = {'client_num': client_num,
                'parameters': parameters,
                'tasks': tasks,
                '_pg_trigger': _pg_trigger,
                '_pg_action': _pg_action,
                '_pg_storageformat': _pg_storageformat,
                '_pg_storage': _pg_storage,
                'logger': _logger,
                'args': args,
                'kwargs': kwargs}

        _logger.info(_var)
        # print(f"action: {_pg_action}")
        #print(f"db: {_pg_db}")
        #print(f"parameters: {parameters}")

        if _pg_action is None:
            pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, "Nothing to do")
            return

        ### Step 1, Get Tasks
        if _pg_trigger:
            _trigger_tasks = tasks or _pg_trigger[parameters['_pg_trigger']].get_tasks()
            _logger.info(f"tasks: {_trigger_tasks}")
            for _data_index, _data_element in enumerate(_trigger_tasks):
                if (_data_index % parameters['num_client']) == client_num:
                    _pg_trigger[parameters['_pg_trigger']].process(_data_element)

        processed_list = []

        ### Step 2, Process Task
        _action_tasks = None

        if _pg_action:
            if _pg_action[parameters['_pg_action']].get_tasks(parameters['input_file'], parameters):
                _action_tasks = _pg_action[parameters['_pg_action']]._data_inputs
            else:
                pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, "unable to get tasks")
            print(f"here is the data input: {_action_tasks[0]}")

            if _action_tasks:
                for _data_index, _data_element in enumerate(_action_tasks):
                    if (_data_index % parameters['num_client']) == client_num:
                        _pg_action[parameters['_pg_action']]._process(*_data_element)
                        processed_list.append(f"{client_num}: {_data_element}")

                pprint(_pg_action[parameters['_pg_action']].data)

        _pg_task_result = _pg_action[
            parameters['_pg_action']].data if _pg_action and "_pg_action" in parameters else None
        ### Step 3, Encode Result
        if _pg_storageformat:
            pass

        ### Step 4, Store Result
        if _pg_storage and _pg_task_result:
            pass
        elif _pg_db and _pg_task_result:
            list(map(lambda x: _pg_db[f"db_{parameters['_pg_db']}_default"].pg_save('_'.join(x[0].split('_')[:-1]),
                                                                                    _pg_action[
                                                                                        parameters['_pg_action']].data[
                                                                                        x[0]],
                                                                                    parameters['exception_filepath']),
                     _pg_task_result.items()))

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)


async def get_data(_pg_action=None):
    _process_object_map = {'_pg_trigger': {},
                           '_pg_action': pglearning.pg_learning,
                           '_pg_storageformat': {},
                           '_pg_storage': {},
                           # '_pg_db': pgdatabase.pg_db_session
                           '_pg_db': {}
                           }

    _process_object_parameters = {'_pg_trigger': (),
                                  '_pg_action': ("ner", "", "_pg_action", 999),
                                  '_pg_storageformat': (),
                                  '_pg_storage': (),
                                  # '_pg_db': ("mysql", "", "_pg_db", {}, 999)
                                  '_pg_db': ()
                                  }

    _parameters = {
        'separator': "|",
        'name': "reddit_investing_new",
        'input_file': "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/SocialMedia/Example/investing_new_1f732339_.csv"
        # 'read_func': pgscrapyformatter.PGScrapFormatter().redfin_address_format,
        # 'address_file': "/Users/jianhuang/opt/anaconda3/envs/Data20/rl_data/real_estate/test_v2/output.json"
        }
    # 'address_file': "/Users/jianhuang/opt/anaconda3/envs/Data20/rl_data/real_estate/test_v2/output_test.json"}

    _func_map = [(val, _process_object_parameters[key]) for key, val in _process_object_map.items() if val]
    _func_parameters = {key: val[0] for key, val in _process_object_parameters.items() if val}

    _news_proc = pg_worker_v2

    for _dec in list(map(lambda x: x[0](*x[1]) if x[1] else x[0](), _func_map)): _news_proc = _dec(_news_proc)
    total_client_number = 5
    news_run_tasks = pgprocessing.pg_asyncio()(
        pgprocessing.pg_multithreading(num_workers=total_client_number)(pgprocessing.pg_run_tasks))
    ret = await news_run_tasks(func=_news_proc, num_client=total_client_number,
                               parameter={**_func_parameters, **_parameters})
    print(ret)


def main():
    _loop = asyncio.get_event_loop()
    try:
        _loop.run_until_complete(get_data())

    except Exception as err:
        print(err)
        return None


def test(_pg_action=None, _pg_db=None):
    print(_pg_action)
    print(_pg_db)


if __name__ == '__main__':
    # pgdatabase.pg_db_session("mysql", "", "_pg_db", {}, 999)(test)()
    main()
