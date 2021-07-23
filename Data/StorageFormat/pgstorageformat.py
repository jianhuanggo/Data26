"""
Exclusively for the purpose of managing daemon
"""

import contextlib
import functools
import inspect
import json
from types import SimpleNamespace
from typing import Callable, Any, TypeVar

import pandas as pd

from Data.StorageFormat import pgcsv
from Data.StorageFormat import pgjson
from Data.StorageFormat.Blockchain import pgblockchaingeneral
from Data.StorageFormat.Excel import pgexcel
from Meta import pggenericfunc
from Data.Utils import pgyaml

__version__ = "1.7"

F = TypeVar('F', bound=Callable[..., Any])

"""
mypackage = __import__('mypackage')
mymodule = getattr(mypackage, 'mymodule')
myfunction = getattr(mymodule, 'myfunction')

myfunction(parameter1, parameter2)

"""

@contextlib.contextmanager
def pg_set_storage_format(file, _logger=None):
    _storage_format = {'csv': pd.read_csv,
                       'json': json.loads,
                       'yaml': pgyaml.yaml_load
    }
    print(file)
    for key, item in _storage_format.items():
        try:
            yield {"file_format": key,
                   "data": item(file)
                   }
        except Exception as err:
            continue

@contextlib.contextmanager
def set_storage_format(object_type: str,
                       object_name: str = None,
                       storage_format_parameter: dict = None,
                       storage_instance: dict = None,
                       subscription_level=1,
                       _logger=None):

    action = {'blockchain':     {'1': pgblockchaingeneral.PGBlockChainGeneral(),
                                 '2': pgblockchaingeneral.PGBlockChainGeneral(),
                                 '999': pgblockchaingeneral.PGBlockChainGeneral(),
                                 },
              'excel':          {'1': pgexcel.PGExcel(),
                                 '2': pgexcel.PGExcel(),
                                 '999': pgexcel.PGExcel(),
                                 },
              }

    parameter = {'blockchain':  {'1': {},
                                 '2': {},
                                 '999': {},
                                 },
                 'excel':       {'1': {},
                                 '2': {},
                                 '999': {},
                                 },
                 }

    try:
        if object_type not in action:
            pggenericfunc.pg_error_logger(_logger,
                                          inspect.currentframe().f_code.co_name,
                                          f"{object_type} not found, currently {', '.join(action.keys())} are supported")
            raise

        instance_session = action.get(object_type, None)[str(subscription_level)]
        if instance_session and storage_format_parameter:
            instance_session.set_param(**parameter.get(object_type, None)[str(subscription_level)])

        print(f"storage_instance111: {storage_instance}")
        for storage_inst in storage_instance.values():
            #for val in storage_inst.values():
                storage_inst._storage_format_instance[f"storage_format_{object_type}_{object_name}"] = instance_session

        _name = f"storage_format_{object_type}_{object_name}" if object_name else f"storage_format_{object_type}"
        instance_session.set_param(storage_instance=storage_instance,
                                   name={'name': _name})

        yield instance_session if instance_session else None

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)


def pgstorageformat(object_type: str,
                    object_name: str = "default",
                    variable_name: str = "_pg_action",
                    storage_format_parameter: dict = None,
                    subscription_level: int = 1) -> Callable[[F], F]:

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = variable_name
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            # print(session_in_args)
            # print(session_in_kwargs)
            # print(kwargs)

            # if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name]
            #    if object_name is None or object_name in kwargs[variable_name][object_type]:

            if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name]:
                if object_name is None or object_name in kwargs[variable_name][object_type]:
                    return func(*args, **kwargs)
            else:
                _storage_instance = {}
                if variable_name in kwargs:
                    for _key in kwargs[variable_name].keys():
                        if _key.startswith("storage") and not _key.startswith("storage_format"):
                            _storage_instance[_key] = kwargs[variable_name][_key]
                print(_storage_instance)
                # if arg_session in inspect.getfullargspec(func).args:

                with set_storage_format(object_type=object_type,
                                        object_name=object_name,
                                        storage_format_parameter=storage_format_parameter,
                                        storage_instance=_storage_instance,
                                        subscription_level=subscription_level) as session:
                    # print(session)

                    if session:
                        _object_name = object_name or "default"
                        if variable_name in kwargs:
                            #kwargs[variable_name][f"storage_format_{object_type}_{_object_name}"] = {_object_name: session}
                            kwargs[variable_name][f"storage_format_{object_type}_{_object_name}"] = session
                        else:
                            #object_name_namespace = {_object_name: session}
                            #kwargs[variable_name] = {f"storage_format_{object_type}_{_object_name}": object_name_namespace}
                            kwargs[variable_name] = {f"storage_format_{object_type}_{_object_name}": session}

                    """
                    if session:
                        if object_name:
                            object_name_namespace = {object_name: session}
                            kwargs[variable_name] = {f"storage_format_{object_type}": object_name_namespace}
                        else:
                            kwargs[variable_name] = {f"storage_format_{object_type}": {'default': session}}
                    """
                    print(kwargs)
                return func(*args, **kwargs)

        return wrapper

    return decorator



"""
@contextlib.contextmanager
def set_storage_format(storage_format: str, name: str = "default", storage_instance: dict = None, _logger=None):

    action = {'blockchain':       pgblockchaingeneral.PGBlockChainGeneral(),
              'excel':            pgexcel.PGExcel(),
              }

    parameter = {'blockchain':     "reserved",
                 'excel':          "reserved",
                 }

    try:

        if storage_format not in action:
            pggenericfunc.pg_error_logger(_logger,
                                          inspect.currentframe().f_code.co_name,
                                          f"storage format: {storage_format} is not found")
            raise

        instance = action[storage_format]

        #print(f"storage instance: {storage_instance}")
        for storage_inst in storage_instance.values():
            storage_inst._storage_format_instance[f"sformat_{storage_format}_{name}"] = instance

        instance.set_param(storage_instance=storage_instance,
                           name=f"sformat_{storage_format}_{name}")

        yield instance

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)
        raise


def pgstorageformat(storage_format: str, name: str = "default"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = f"sformat_{storage_format}_{name}"
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                _storage_instance = {}
                for _key in kwargs.keys():
                    if _key.startswith("storage"):
                        _storage_instance[_key] = kwargs[_key]

                with set_storage_format(storage_format, name, _storage_instance) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator
"""

if __name__ == '__main__':
    pass
