import time
import asyncio
import inspect
import logging
from pprint import pprint
from Learning import pglearning
from Learning.PyCaret import pgpycaret
from Data.Storage import pgstorage
from Data.Storage.S3 import pgs3
from Processing import pgprocessing
from Meta import pggenericfunc


### scrapy crawl pgsiders -o output.json

def pg_worker_v3(client_num: int, parameters=None, tasks=None,
                 _pg_trigger=None, _pg_action=None, _pg_storageformat=None,
                 _pg_storage=None, _pg_db=None, _logger=None, *args, **kwargs):

    _logger = logging.getLogger('client({})'.format(client_num))

    try:
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
        print(parameters)

        for _pg_task_num, _pg_task_item in parameters["pg_action"].items():
            _pg_task = _pg_task_item()
            if _pg_task.get_tasks("s3://tag-data-app-prod-0001/", parameters):
                _trigger_tasks = _pg_task._data_inputs
                print("okokokok11111")
                print(_trigger_tasks)
            exit(0)

        exit(0)
    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)
#def pgstorage(object_type: str, object_name: str = None, variable_name: str ="_pg_action", storage_parameter: dict = None, subscription_level: int = 1) -> Callable[[F], F]:
#pg_learning(object_type: str, object_name: str, variable_name: str ="_pg_action", subscription_level: int = 1)



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
        print(parameters)
        print(_pg_trigger)

        ### Step 1, Get Tasks
        if _pg_trigger:
            if _pg_trigger[parameters['_pg_trigger']].get_tasks("s3://tag-data-app-prod-0001/", parameters):
                _trigger_tasks = _pg_trigger[parameters['_pg_trigger']]._data_inputs
                _logger.info(f"tasks: {_trigger_tasks}")
            else:
                pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, "unable to get tasks")

            if _trigger_tasks:
                for _data_index, _data_element in enumerate(_trigger_tasks):
                    if (_data_index % parameters['num_client']) == client_num:
                        _pg_trigger[parameters['_pg_trigger']].process(_data_element)

                pprint(f"client {client_num}: {_pg_trigger[parameters['_pg_trigger']].data}")


        processed_list = []
        exit(0)
        ### Step 2, Process Task
        _action_tasks = None

        print("okokok")
        if _pg_action:
            if _pg_action[parameters['_pg_action']].get_tasks(parameters['input_file'], {**parameters, **{'client_id': client_num,
                                                                                                          'num_client': parameters['num_client']}}):
                _action_tasks = _pg_action[parameters['_pg_action']]._data_inputs
            else:
                pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, "unable to get tasks")
            print(f"here is the data input: {_action_tasks}")
            #exit(0)

            if _action_tasks:
                for _data_index, _data_element in enumerate(_action_tasks):
                    if (_data_index % parameters['num_client']) == client_num:
                        _pg_action[parameters['_pg_action']]._process(*_data_element)
                        processed_list.append(f"{client_num}: {_data_element}")

                pprint(f"client {client_num}: {_pg_action[parameters['_pg_action']].data}")

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
#def pgstorage(object_type: str, object_name: str = None, variable_name: str ="_pg_action", storage_parameter: dict = None, subscription_level: int = 1) -> Callable[[F], F]:
#pg_learning(object_type: str, object_name: str, variable_name: str ="_pg_action", subscription_level: int = 1)


async def get_data(_pg_action=None):
    _process_object_map = {'_pg_trigger': pgstorage.pgstorage,
                           '_pg_action': pglearning.pg_learning,
                           '_pg_storageformat': {},
                           '_pg_storage': {},
                           # '_pg_db': pgdatabase.pg_db_session
                           '_pg_db': {}
                           }

    s3_storage_parameter = {"directory": "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2",
                      "aws_access_key_id": "AKIAXW42WHCP3KKG2BPJ",
                      "aws_secret_access_key": "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",
                      "region_name": "us-east-1",
                      "mode": "file",
                      "entity_name": "test"}

    _process_object_parameters = {'_pg_trigger': ("s3", "s3", "_pg_trigger", {}, 999),
                                  '_pg_action': ("caret", "", "_pg_action", 999),
                                  '_pg_storageformat': (),
                                  '_pg_storage': (),
                                  # '_pg_db': ("mysql", "", "_pg_db", {}, 999)
                                  '_pg_db': ()
                                  }

    _test_func_map = {"pg_action": {'0': pgs3.PGS3Ext,
                                    '1': pgpycaret.PGLearningCaretSingleton,
                                    '2': {},}
                      }

    _parameters = {
        #'separator': ",",
        'entity_name': "heart",
        'input_file': "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Input/heart.csv",
        # 'read_func': pgscrapyformatter.PGScrapFormatter().redfin_address_format,
        # 'address_file': "/Users/jianhuang/opt/anaconda3/envs/Data20/rl_data/real_estate/test_v2/output.json"
        "directory": "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2",
        "aws_access_key_id": "AKIAXW42WHCP3KKG2BPJ",
        "aws_secret_access_key": "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",
        "region_name": "us-east-1",
        "mode": "file"
        }
    # 'address_file': "/Users/jianhuang/opt/anaconda3/envs/Data20/rl_data/real_estate/test_v2/output_test.json"}

    _func_map = [(val, _process_object_parameters[key]) for key, val in _process_object_map.items() if val]
    _func_parameters = {key: val[0] for key, val in _process_object_parameters.items() if val}

    #_news_proc = pg_worker_v2
    _news_proc = pg_worker_v3

    for _dec in list(map(lambda x: x[0](*x[1]) if x[1] else x[0](), _func_map)): _news_proc = _dec(_news_proc)

    total_client_number = 1
    news_run_tasks = pgprocessing.pg_asyncio()(
        pgprocessing.pg_multithreading(num_workers=total_client_number)(pgprocessing.pg_run_tasks))
    #ret = await news_run_tasks(func=_news_proc, num_client=total_client_number, parameter={**_func_parameters, **_parameters})
    ret = await news_run_tasks(func=_news_proc, num_client=total_client_number, parameter={**_test_func_map, **_parameters})
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
