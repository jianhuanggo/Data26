import contextlib
from functools import wraps
import multiprocessing
from multiprocessing import Pool
import sys
import os
import inspect
from Meta import pggenericfunc
from Processing import pgprocessing

from Data.Connect import postgresql
from Data.Connect import redshift
from Data.Config import pgconfig

#cpu_count = multiprocessing.cpu_count()
#p = Pool(cpu_count)


#post_url = ""
#tablename = "admins"
#key = "id"
#task_list, path = ext.schedule_multi_extract("", post_url, tablename, key, 0)
#result = p.map(ext.run_multi_extract, task_list)
#p.close()
#p.join()

"""
def connect(db_type):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = 'db_instance'
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with create_session(db_type) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)
        return wrapper
    return decorator




def multiprocess(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        cpu_count = multiprocessing.cpu_count()
        p = Pool(cpu_count)
        print(func)
        #print(sys._getframe().f_code.co_name)
        result = p.map(func.__name__, *args, **kwargs)
        p.close()
        p.join()
        return result
    return wrapper

from multiprocessing import Pool

def decorate_func(f):
    def _decorate_func(*args, **kwargs):
        print "I'm decorating"
        return f(*args, **kwargs)
    return _decorate_func

def actual_func(x):
    return x ** 2

def wrapped_func(*args, **kwargs):
    return decorate_func(actual_func)(*args, **kwargs)

my_swimming_pool = Pool()
result = my_swimming_pool.apply_async(wrapped_func,(2,))
print(result.get())

"""

import random
import multiprocessing
import functools
import subprocess
from Data.Utils import db
from pprint import pprint


"""
mp_io = processing.pg_multiprocessing(num_workers=total_client_number)(processing.pg_asyncio()(run_blocking_tasks))
        parameter = {'mode': 'direct',
                     'bucket_name': 'testba8a4bda',
                     }
"""




def run_query(*, db_client_dbshell: str, db_url: str, query: str):
    try:
        exec_query = [db_client_dbshell, db_url, "-c", query]
        #print(exec_query)

        proc = subprocess.Popen(exec_query, stdout=subprocess.PIPE)
        proc.wait()
        reader = proc.stdout
        if query.split(' ')[0].lower() == 'select':
            result_list = [item.strip() for item in reader.read().decode('utf-8').split('\n')[2].split('|')]
        else:
            result_list = None

    except Exception as e:
        raise e

    return result_list


def wrap_run_query_v2_from_file(*, db_system: str, filepath: str):
    def run_query_v2_from_file(*, fpath: str, db_instance=None):
        with open (fpath, 'r') as f:
            for item in db_instance.session.execute(f.read()).fetchall():
                pprint(dict(item))

    if db_system.lower() in ('rds', 'meta', 'redshift'):
        run_query_v2_from_file = db.connect(db_system.lower())(run_query_v2_from_file)
        run_query_v2_from_file(fpath=filepath)
    else:
        raise ("The support db_system is rds, meta and redshift")


def create_tbl_by_cp_final_tbl(*, db_client_dbshell: str, db_url: str, schema_name: str, stage_tbl_name: str, final_tbl_name: str):
    query = f"create table {schema_name}.{stage_tbl_name} as select * from {final_tbl_name} limit 1"

    try:
        if query:
            run_query(db_client_dbshell=db_client_dbshell, db_url=db_url, query=query)

    except Exception as e:
        print(e)
        raise e


def is_table_exist(*, db_type: str, table_name: str, db_client_dbshell: str, db_url: str, schema_name:str):

    query = None
    result = None

    if db_type.lower() == 'redshift':
        query = f"SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE " \
                f"table_schema = '{schema_name}' AND table_name = '{table_name}');"

    try:
        if query:
            result = run_query(db_client_dbshell=db_client_dbshell, db_url=db_url, query=query)

    except Exception as e:
        raise e

    if result[0] == 't':
        return True
    else:
        return False


def system_process_execute():
    db_client_dbshell = ''
    redshift_url = ''
    apply_query = ''
    loadConf = [db_client_dbshell, redshift_url, "-c", apply_query]
    #self.args.logger.debug(loadConf)

    p2 = subprocess.Popen(loadConf)
    p2.wait()
    if p2.returncode != 0:
        return False


def system_process_return():

    db_client_dbshell = ''
    #postgresql_url = f"postgresql://{username}:{passwd}@{host}:{port}/{database}"
    postgresql_url = ''
    query = ''

    exec_query = [db_client_dbshell, postgresql_url, "-c", query]

    proc = subprocess.Popen(exec_query, stdout=subprocess.PIPE)
    proc.wait()
    reader = proc.stdout
    result_list = [item.strip() for item in reader.read().decode('utf-8').split('\n')[2].split('|')]


class my_decorator(object):
    def __init__(self, target):
        self.target = target
        try:
            functools.update_wrapper(self, target)
        except:
            pass

    def __call__(self, candidates):
        f = []
        for candidate in candidates:
            #f.append(self.target([candidate], args)[0])
            f.append(self.target([candidate]))

        return f


def old_my_func(candidates, args):
    f = []
    for c in candidates:
        f.append(sum(c))
    return f


def get_param_in_list(input_param) -> list:
    if not input_param:
        return []
    elif type(input_param) == list:
        return input_param
    else:
        input_param = str(input_param)
        input_param = input_param.replace(' ', '')
        return input_param.strip().split(',')


my_func = my_decorator(old_my_func)

if __name__ == '__main__':
    candidates = [[random.randint(0, 9) for _ in range(5)] for _ in range(10)]
    print(candidates)
    pool = multiprocessing.Pool(processes=4)
    results = [pool.apply_async(my_func, ([c], {})) for c in candidates]
    print(results)
    pool.close()
    f = [r.get()[0] for r in results]
    print(f)

