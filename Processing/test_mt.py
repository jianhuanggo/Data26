import requests
import random
from multiprocessing import cpu_count
from Data.Utils import pgtimeit
from Processing import pgprocessing

__version__ = "1.5"


def mul(num1, num2, num3=5):
    print(locals())
    print(num1 * num2 * num3)


URL = 'https://httpbin.org/uuid'


def fetch(session, url):
    with session.get(url) as response:
        print(response.json()['uuid'])


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multithreading(num_workers=15, chunksize=5)
def test1_mt(executor=None):
    variable_list = []
    for i in range(100):
        variable_list.append((random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))
    executor.run(mul, variable_list)


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multithreading(num_workers=15, chunksize=10)
def test1_mt_native_ordered(executor=None):
    _num_iteration = 100
    executor.run_native_ordered(mul, ([random.randint(1, 100) for _ in range(_num_iteration)],
                                  [random.randint(1, 100) for _ in range(_num_iteration)],
                                  [random.randint(1, 100) for _ in range(_num_iteration)]))


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multithreading(num_workers=15)
def test1_mt_native_unordered(executor=None):
    variable_list = []
    for i in range(100):
        variable_list.append((random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))

    executor.run_native_unordered(mul, variable_list)


@pgtimeit.timer(1, 1)
@pgprocessing.pg_multithreading(num_workers=10)
def test2_mt(executor=None):
    """
    [(arg1, arg2), (arg1, arg2)...]
    """
    with requests.Session() as session:
        executor.run(fetch, [(session, URL) for _ in range(100)])


if __name__ == '__main__':
    #print(cpu_count())
    test1_mt()
    #test1_mt_native_ordered()
    #test1_mt_native_unordered()
    test2_mt()

