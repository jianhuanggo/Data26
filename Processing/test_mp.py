import requests
import random
import itertools
from pprint import pprint
import multiprocessing
from multiprocessing import cpu_count
from Data.Utils import pgtimeit
from Processing import pgprocessing
from multiprocessing import Pool

__version__ = "1.5"


@pgtimeit.timer(1, 1)
def test1_serial(executor=None):
    variable_list = []
    for i in range(100):
        variable_list.append((random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))

    for item in variable_list:
        mul(*item)


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multiprocessing(num_workers=4, is_daemon=False, chunksize=25)
def test1_mp_map(executor=None):
    variable_list = []
    for i in range(100):
        variable_list.append((random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))
    executor.run_map(mul, variable_list)


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multiprocessing()
def test1_mp_async(executor=None):
    variable_list = []
    for i in range(100):
        variable_list.append((random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))
    executor.run_async(mul, variable_list)


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multiprocessing()
def test1_mp(executor=None):
    variable_list = []
    for i in range(100):
        variable_list.append((random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))
    executor.run(mul, variable_list)


def mul(num1, num2, num3=5):
    print(locals())
    print(num1 * num2 * num3)


URL = 'https://httpbin.org/uuid'


def fetch(session, url):
    with session.get(url) as response:
        print(response.json()['uuid'])


@pgtimeit.timer(1, 1)
def test2_serial(pool=None):
    """
    [(arg1, arg2), (arg1, arg2)...]
    """
    with requests.Session() as session:
        for _ in range(100):
            fetch(session, URL)


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multiprocessing(num_workers=10, is_daemon=False)
def test2_mp(executor=None):
    """
    [(arg1, arg2), (arg1, arg2)...]
    """
    with requests.Session() as session:
        executor.run(fetch, [(session, URL) for _ in range(100)])


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multiprocessing(num_workers=10, is_daemon=False)
def test2_mp_async(executor=None):
    """
    [(arg1, arg2), (arg1, arg2)...]
    """
    with requests.Session() as session:
        executor.run_async(fetch, [(session, URL) for _ in range(100)])


def sync_code(cb):
    for i in range(10):
        cb(i)


def call_fetch_api():
    fetch_api("good_project", "api", payload={'payload': "nice"}, headers={"api": "aaa"}, key=True)
    args_iter = zip(itertools.repeat("good project", 5), "zip")
    kwargs_iter = itertools.repeat(dict(payload={'a': 1}, key=True), 5)
    print(list(args_iter))
    print(list(kwargs_iter))
    with NestablePool(4) as pool:
        branches = starmap_with_kwargs(pool, fetch_api, args_iter, kwargs_iter)


def fetch_api(project_name, api_extensions, payload={}, headers={}, API_LINK="API_LINK", key=False):
    print(locals())


def starmap_with_kwargs(pool, fn, args_iter, kwargs_iter):
    print(locals())
    args_for_starmap = zip(itertools.repeat(fn), args_iter, kwargs_iter)
    print(args_for_starmap)
    return pool.starmap(apply_args_and_kwargs, args_for_starmap)


def apply_args_and_kwargs(fn, args, kwargs):
    print(locals())
    return fn(*args, **kwargs)




def b():
    #a([1]*2, [2]*2)
    z([1]*2, [2]*2)


def g():
    ok = [(27, 86, 25),(70, 54, 23)]
    z(*ok)


def c():
    _input = [(1, 2) for _ in range(2)]
    print(_input)

    _result = list(map(list, itertools.zip_longest(*_input, fillvalue=None)))
    print(_result)
    a(*_result)


def a(*iterables):
    pprint(iterables)


def z(*iterables):
    print(iterables)
    args_list = list(map(tuple, itertools.zip_longest(*iterables, fillvalue=None)))
    print(f"arg_list: {args_list}")
    args_newlist = [args for args in zip(*iterables)]
    print(f"arg_newlist: {args_newlist }")


def s():
    sss = {'num1': 2, 'num2': 5}
    y(sss)


def y(*iterables):
    print(iterables)
    kw = {"arg1": "Geeks", "arg2": "for", "arg3": "Geeks"}
    t(**kw)


def t(**kwargs):
    print(kwargs)
    #print(num1)
    #print(num2)


def f():
    b = {'num1': 5, 'num2': 10, 'num3': 10}
    mul(**b)


if __name__ == '__main__':

    print(cpu_count())
    #call_fetch_api()
    test1_serial()
    test1_mp()
    test1_mp_async()
    test1_mp_map()
    test2_serial()
    test2_mp()
    test2_mp_async()
    #print(resultss)
    #b()
    #c()
    #g()
    #s()
    #f()
