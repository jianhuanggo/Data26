import time
import random
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

"""
if _pg_action:
    if _pg_action[parameters['_pg_action']].get_tasks(parameters['input_file'],
                                                      {**parameters, **{'client_id': client_num,
                                                                        'num_client': parameters['num_client']}}):
        _action_tasks = _pg_action[parameters['_pg_action']]._data_inputs
    else:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, "unable to get tasks")
    print(f"here is the data input: {_action_tasks}")
    # exit(0)

    if _action_tasks:
        for _data_index, _data_element in enumerate(_action_tasks):
            if (_data_index % parameters['num_client']) == client_num:
                _pg_action[parameters['_pg_action']]._process(*_data_element)
                processed_list.append(f"{client_num}: {_data_element}")

        pprint(f"client {client_num}: {_pg_action[parameters['_pg_action']].data}")

_pg_task_result = _pg_action[
    parameters['_pg_action']].data if _pg_action and "_pg_action" in parameters else None
"""


def pg_nested_worker(client_num: int, parameters=None, tasks=None, *args, **kwargs):
    _logger = logging.getLogger('client({})'.format(client_num))
    _combined_param = {**parameters, **{'pg_client_id': client_num, 'pg_total_client_num': parameters['num_client']}}
    print(_combined_param)
    #time.sleep(random.randint(0, 7))
    _pg_data = None
    try:
        for _pg_task_num, _pg_task_item in parameters["pg_action"].items():
            _pg_task = _pg_task_item()
            _pg_data = _pg_data or _combined_param["pg_input_data"]
            print(f"{_pg_task_num}_pg_data: {_pg_data}")

            if not _pg_data:
                return True
            elif _pg_task.get_tasks(_pg_data, _combined_param) and _pg_task._data_inputs:
                #for _data_index, _data_element in enumerate(_pg_task._data_inputs):
                #    if (_data_index % parameters['num_client']) == client_num:
                print(f"\n\n\n\n\nxxxxxxx {_pg_task._data_inputs} xxxxxxx\n\n\n\n\n")
                print(f"\n\n\n\n\nyyyyyyy {_pg_task._data} yyyyyyy\n\n\n\n\n\n")
                _pg_task.process()
                #_pg_task.process(_combined_param["entity_name"], _combined_param)
                print(f"\n\n\n\n\nzzzzz {_pg_task._data} zzzzz\n\n\n\n\n\n")
                _pg_data = _pg_task._data
            elif not _pg_task._data_inputs:
                print(f"\n client id {client_num}, data : {_pg_task._data_inputs}\n")
                return True
            else:
                print("In exception")
                print(f"{_pg_task_num}_data_input: {_pg_task._data_inputs}")
                print(f"{_pg_task_num}_pg_data: {_pg_task._data}")
                print("something is wrong!!!!")
                exit(0)

            print(f"\n client id {client_num}, data : {_pg_data}\n")
            exit(0)
        return _pg_task._data
    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)
        return False


async def get_data(_pg_action=None):

    _test_func_map = {"pg_action": {'0': pgs3.PGS3Ext,
                                    '1': pgpycaret.PGLearningCaretSingleton,
                                    }
                      }

    _parameters = {
        #'separator': ",",
        'entity_name': "heart",
        #'input_file': "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Input/heart.csv",
        # 'read_func': pgscrapyformatter.PGScrapFormatter().redfin_address_format,
        # 'address_file': "/Users/jianhuang/opt/anaconda3/envs/Data20/rl_data/real_estate/test_v2/output.json"
        "directory": "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2",
        "aws_access_key_id": "AKIAXW42WHCP3KKG2BPJ",
        "aws_secret_access_key": "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",
        "region_name": "us-east-1",
        "mode": "file",
        "pg_input_data": "s3://tag-data-app-prod-0001/"
        }

    total_client_number = 1
    news_run_tasks = pgprocessing.pg_asyncio()(
        pgprocessing.pg_multithreading(num_workers=total_client_number)(pgprocessing.pg_run_tasks))
    ret = await news_run_tasks(func=pg_nested_worker, num_client=total_client_number, parameter={**_test_func_map, **_parameters})
    print(ret)


def main():
    _loop = asyncio.get_event_loop()
    try:
        _loop.run_until_complete(get_data())

    except Exception as err:
        print(err)
        return None


if __name__ == '__main__':
    main()
