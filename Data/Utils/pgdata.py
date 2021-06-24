import inspect
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Meta import pggenericfunc


def diff_list(a: Union[list, set, tuple], b: Union[list, set, tuple], _logger=None) -> Union[set, None]:
    try:
        return set(a) ^ set(b)

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)
    return None
