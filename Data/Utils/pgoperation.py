import sys
import time
import inspect
import functools
from itertools import zip_longest
from typing import Callable, Tuple
import logging
from pprint import pprint
from Meta import pggenericfunc
from Meta.pggenericfunc import check_args


def pg_ignore_fail(pg_value: bool, func: str, ignore_flg: bool = True, addition_msg: str = None, logger=None) -> bool:
    if not pg_value and not ignore_flg:
        pggenericfunc.pg_error_logger(logger, func, addition_msg)
        sys.exit(99)
    else:
        return pg_value


def pg_retry(rety_num: int = 1, validation_func: Callable = None) -> Callable:
    def default_validation_func(x) -> bool:
        return True if x else False

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(rety_num):
                try:
                    _ret = func(*args, **kwargs)
                    _validation_func = default_validation_func if validation_func is None else validation_func
                    if _validation_func(_ret):
                        return _ret
                    else:
                        time.sleep(5)

                except Exception as error:
                    print(error)
                    continue
        return wrapper
    return decorator


def pg_try_catch(rety_num: int = 1, validation_func: Callable = None, logger: logging.Logger = None) -> Callable:
    def default_validation_func(x) -> bool:
        return True if x else False

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print(f"this is a func: {func}")
            #print({x[0]: x[1] for x in zip_longest((k for k, v in inspect.signature(func).parameters.items() if v.default is inspect.Parameter.empty), args) if x[0]})
            check_args(func.__name__, {x[0]: x[1] for x in zip_longest((k for k, v in inspect.signature(func).parameters.items() if v.default is inspect.Parameter.empty), args) if x[0]}, False)
            for _ in range(rety_num):
                try:
                    _ret = func(*args, **kwargs)
                    _validation_func = default_validation_func if validation_func is None else validation_func
                    if rety_num == 1 or _validation_func(_ret):
                        return _ret
                    else:
                        time.sleep(5)

                except Exception as error:
                    pggenericfunc.pg_error_logger(logger, func.__name__, error)
                    continue
        return wrapper
    return decorator


def pg_operation_run(method_dict: dict,
                     method_cnt: str,
                     method_validator: Callable,
                     method_parameters: dict = None,
                     logger=None) -> Tuple[str, dict]:

    try:
        while int(method_cnt) < len(method_dict):
            print(f"attempt {method_cnt}: {method_dict.get(method_cnt, 'no more methods')}")
            if method_validator(method_dict.get(method_cnt, "no more methods")):
                if method_cnt != '0':
                    method_dict['0'], method_dict[method_cnt] = method_dict[method_cnt], method_dict['0']
                return method_dict.get(method_cnt), method_dict
            else:
                method_cnt = str(int(method_cnt) + 1)
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)


class PGMetaClass(type):
    """
    A MetaClass to facilitate the creation of classes and apply common funnctionalities to
    their methods

    Attributes
    ----------

    Methods
    -------
    __new__(mcs, name, bases, local)
        Apply decorator pg_try_catch to the methods of classes which inherited this metaclass
    """
    def __new__(mcs, name, bases, local):
        for _method in (attr for attr in local if callable(local[attr])):
            #print(f"the method is: {_method}")
            #print(f"the method is: {local[_method]}")
            local[_method] = pg_try_catch()(local[_method])
        return type.__new__(mcs, name, bases, local)


def get_default_args(func):
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


class test(metaclass=PGMetaClass):

    def __init__(self):
        print("init")

    def x1(self, x, y=None):
        print(1)

    def y1(self):
        print(2)

    def test111(self):
        print(inspect.getmembers(self, predicate=inspect.ismethod)[3])


class myClass(metaclass=PGMetaClass):

    def baz(self, num):
        print("ok")


if __name__ == '__main__':

    _parameters = {'input_string': "300 lan 192, ",
                   'city_name': "Johns Creek",
                   'state_abbr': "GA"

    }
    _methods = {'0': _parameters['input_string'].split(',')[0] + ', ' + ' '.join([i for i in _parameters['input_string'].split(',')[1].split() if i.isdigit()]),
                '1': _parameters['input_string'].replace(f"{_parameters['city_name'].replace('-', ' ')}, {_parameters['state_abbr']} ", ""),
                   #'2': re.search(f"{self._pattern_match['address_parser']['prefix']}(.*){self._pattern_match['address_parser']['surfix']}", input_string).group(1),
                '3': _parameters['input_string'].replace("Unit ", "#")
                  }


    exit(0)
    quux = myClass()
    quux.baz(1)
    test1 = test()
    test1.test111()



