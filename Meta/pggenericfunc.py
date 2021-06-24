import sys
import inspect
from logging import Logger
from typing import Callable, Iterator, TypeVar, Optional, Type, Union
import pandas as pd



__version__ = "1.7"

F = TypeVar('F')


def pg_errorcheck_exist(iter, iterables: Iterator) -> bool:
    if iter is None or iterables is None:
        return False
    _logger = None
    try:
        if isinstance(iter, str):
            _iter = iter.split()
        else:
            _iter = iter
        if set(_iter).issubset(set(iterables)):
            return True
        else:
            return False
    except Exception as err:
        if _logger:
            _logger.Critical(f"Error in {inspect.currentframe().f_code.co_name}! {err}")
        else:
            print(f"Error in {inspect.currentframe().f_code.co_name}! {err}")
        return False


def pg_error_logger(logger: Logger,
                    func: str,
                    error,
                    addition_msg: str = None,
                    ignore_flag: bool = True) -> None:
    """Display error message in a logger if there is one otherwise stdout

    Args:
        logger: Associated logger if there is one
        func: calling function, so error msg can be associated correctly
        error: exception captured
        addition_msg: A set of parameters which need to be verified
        ignore_flag: It will return to the calling function if set to True otherwise program will terminate

    Returns:
        No return value.

    """
    if logger:
        logger.Critical(f"Error in {func}! {addition_msg} {error}")
    else:
        print(f"Error in {func}! {addition_msg} {error}")

    if not ignore_flag:
        sys.exit(99)
    return None


def check_args(func: str, parameters: dict, ignore_flag: bool = True, logger: Logger = None):
    """Returns True if parameters are not None or empty

    Args:
        func: calling function, so error msg can be associated correctly
        parameters: A set of parameters which need to be verified
        ignore_flag: It will return to the calling function if set to True otherwise program will terminate
        logger: Associated logger if there is one

    Returns:
        The return value. True for success, False otherwise.

    """
    def _check_data_type(data: any) -> bool:
        if isinstance(data, pd.DataFrame):
            return True if not data.empty else False
        else:
            return True if data else False

    try:
        _invalid_parameters = [_key for _key, _val in parameters.items() if not _check_data_type(_val)]
        return pg_error_logger(logger, func, "", f"{_invalid_parameters} have null, empty value", ignore_flag) if _invalid_parameters else True
    except Exception as err:
        if logger:
            logger.Critical(f"Error in {inspect.currentframe().f_code.co_name}! {err}")
        else:
            print(f"Error in {inspect.currentframe().f_code.co_name}! {err}")
        sys.exit(99)


def notimplemented():
    raise NotImplementedError("This function is not implemented")


if __name__ == '__main__':
    dict1 = {'name': "hello",
             'age': 17}
    list1 = ["name"]


    print(pg_errorcheck_exist("name", dict1))
    print(pg_errorcheck_exist("name", list1))
    print(pg_errorcheck_exist("name age", dict1))
