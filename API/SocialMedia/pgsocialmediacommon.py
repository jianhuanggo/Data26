import inspect
import contextlib
import functools
from types import SimpleNamespace
from typing import Callable, Any, TypeVar
from Meta import pggenericfunc, pgclassdefault


__version__ = "1.7"

F = TypeVar('F', bound=Callable[..., Any])

def create_a_function(func, name, *args, **kwargs):
    def function_template(*args, **kwargs):
        func(*args, **kwargs)
    return function_template


class PGSocialMediaCommon(pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str,
                       object_short_name: str,
                       config_file_pathname: str,
                       logging_enable: str,
                       config_file_type: str):

        super().__init__(project_name=project_name,
                         object_short_name=object_short_name,
                         config_file_pathname=config_file_pathname,
                         logging_enable=logging_enable,
                         config_file_type=config_file_type)



