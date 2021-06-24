import os
import inspect
import functools
import contextlib
import logging
from typing import Callable, Union, Tuple, List, Any
from concurrent import futures
from Meta import pggenericfunc
from pprint import pprint

from Processing.Multiprocessing import pgmultiprocessing
from Processing.Multithreading import pgmultithreading
from Processing.Coroutines import pgcoroutines

__version__ = "1.5"


@contextlib.contextmanager
def pg_create_pool(process_mode: str, *args, **kwargs):
    action = {'multiprocessing':    pgmultiprocessing.PGMultiProcessing,
              'multithreading':     pgmultithreading.PGMultiThreading,
              'asyncio':            pgcoroutines.PGCoroutines
              }

    parameter = {"multiprocessing":         '_conf.setup_db',
                 }

    executor = None
    try:
        #pool = action["multiprocessing"](parameter[db_name](db_name))
        print(f"parallelism mode: {process_mode}")
        executor = action[process_mode]()
        executor.set_param(**kwargs)
        yield executor
    except Exception as err:
        raise Exception(f"Something wrong with the session {err}")


def pg_multiprocessing(*arguments, **kwds):
    if arguments:
        raise Exception(f"ambiguous argument(s) {arguments}")
    """
    pool_size: is number processes or threads assign to the pool

    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            arg_session = 'executor'
            func_params = func.__code__.co_varnames
            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                if isinstance(kwargs[arg_session], pgcoroutines.PGCoroutines):
                    if "num_workers" in kwds:
                        _num_of_workers = kwds['num_workers']
                    else:
                        _num_of_workers = 1
                    kwargs[arg_session].set_param(pool_executor=futures.ProcessPoolExecutor(max_workers=_num_of_workers),
                                                  num_workers=_num_of_workers)
                return func(*args, **kwargs)
            else:
                with pg_create_pool("multiprocessing", **kwds) as pool:
                    kwargs[arg_session] = pool
                    return func(*args, **kwargs)
        return wrapper
    return decorator


def pg_multithreading(*arguments, **kwds):
    if arguments:
        raise Exception(f"ambiguous argument(s) {arguments}")
    """
    pool_size: is number processes or threads assign to the pool

    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            arg_session = 'executor'
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs
            if session_in_kwargs or session_in_args:
                if isinstance(kwargs[arg_session], pgcoroutines.PGCoroutines):
                    if "num_workers" in kwds:
                        _num_of_workers = int(kwds['num_workers'])
                    else:
                        _num_of_workers = 1
                    kwargs[arg_session].set_param(pool_executor=futures.ThreadPoolExecutor(max_workers=_num_of_workers),
                                                  num_workers=_num_of_workers)
                return func(*args, **kwargs)
            else:
                with pg_create_pool("multithreading", **kwds) as pool:
                    kwargs[arg_session] = pool
                    return func(*args, **kwargs)
        return wrapper
    return decorator


def pg_asyncio(*arguments, **kwds):
    if arguments:
        raise Exception(f"ambiguous argument(s) {arguments}")
    """
    pool_size: is number processes or threads assign to the pool

    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            arg_session = 'executor'
            func_params = func.__code__.co_varnames
            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                if isinstance(kwargs[arg_session], (pgmultiprocessing.PGMultiProcessing,
                                                    pgmultithreading.PGMultiThreading)):
                    if "num_workers" not in kwds:
                        kwds['num_workers'] = int(kwargs[arg_session]._num_workers)
                    if "pool_executor" not in kwds:
                        if isinstance(kwargs[arg_session], pgmultiprocessing.PGMultiProcessing):
                            kwds['pool_executor'] = futures.ProcessPoolExecutor(max_workers=kwds['num_workers'])
                        elif isinstance(kwargs[arg_session], pgmultithreading.PGMultiThreading):
                            kwds['pool_executor'] = futures.ThreadPoolExecutor(max_workers=kwds['num_workers'])
                    with pg_create_pool("asyncio", **kwds) as pool:
                        kwargs[arg_session] = pool
                return func(*args, **kwargs)
            else:
                with pg_create_pool("asyncio", **kwds) as pool:
                    kwargs[arg_session] = pool
                    return func(*args, **kwargs)
        return wrapper
    return decorator


def multi_filedir_processing(processor, directory, *args, **kwargs):
    map(lambda x: processor(x), directory)


# -> Union[Generator[str, str], None]
def multi_filedir_driver(dirpath: str, total_client_number=6):
    mp_io = pg_multiprocessing(num_workers=total_client_number)(pg_asyncio()(multi_filedir_processing))
    for entry in os.scandir(dirpath):
        if entry.is_dir(follow_symlinks=False):
            yield from multi_filedir_driver(dirpath=entry.path)
        else:
            mp_io()



"""
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
"""


def pg_simple_worker(client_num: int, parameters=None, tasks=None,
                     _pg_trigger=None, _pg_action=None, _pg_storageformat=None,
                     _pg_storage=None, _logger=None, *args, **kwargs):

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

        print(_pg_trigger[parameters['_pg_trigger']].data['crypto_symbol']['BTC-USD'].head())

        exit(0)
            #pprint(_pg_trigger[parameters['_pg_trigger']].data.)
            #print(_pg_trigger[parameters['_pg_trigger']].data.head())


        processed_list = []

        ### Step 2, Process Task
        if _pg_action:
            _action_tasks = _pg_action[parameters['_pg_action']].get_tasks()
            for _data_index, _data_element in enumerate(_action_tasks):
                #_total_client_number = parameters['num_client']
                #do_run = True if (_data_index % parameters['num_client']) == client_num else False

                if (_data_index % parameters['num_client']) == client_num:
                    _pg_action[parameters['_pg_action']].process(_data_element)

                    processed_list.append(f"{client_num}: {_data_element}")
                    #log.info(f"processed url {_data_element}")

            print(_pg_action[parameters['_pg_action']].data)
        ### Step 3, Encode Result
        if _pg_storageformat:
            pass


        ### Step 4, Store Result
        if _pg_storage:
            pass

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)


async def pg_run_tasks(func: Callable, num_client: int, parameter: dict=None, executor=None) -> list:

    variable_list = []
    _parameter = {'num_client': num_client} if parameter is None else {**{'num_client': num_client}, **parameter}
    for i in range(num_client):
        variable_list.append((i, _parameter))

    result = await executor.run(func, *variable_list)
    return result


#@pgstorage.pgstorage(storage_type='s3')
#logger=logging.getLogger('run_blocking_tasks')
@pg_asyncio()
@pg_multithreading(num_workers=6)
async def run_blocking_tasks(func: Callable, num_client: int, data, parameter: dict, storage=None, executor=None) -> list:
    #random_string = str(uuid.uuid4().hex[:8])
    #storage.create_bucket(f"test{random_string}")

    """
    parameter = {'test': 10,
             'mode': 'direct',
             'bucket_name': 'testba8a4bda'}
    """
    result = None
    for _index, _data in enumerate(data):
        result = await executor.run(func, *[(_index % num_client, _data, parameter)])
    return result
