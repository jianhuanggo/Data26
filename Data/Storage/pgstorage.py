"""
Exclusively for the purpose of managing daemon
"""

import contextlib
import functools
import inspect
from types import SimpleNamespace
from typing import Callable, Any, TypeVar
from pprint import pprint
from Meta import pggenericfunc
from Data.Storage.Localdisk import pglocaldisk
from Data.Storage.S3 import pgs3
from Data.StorageFormat import pgstorageformatbase

F = TypeVar('F', bound=Callable[..., Any])


__version__ = "1.7"


"""
mypackage = __import__('mypackage')
mymodule = getattr(mypackage, 'mymodule')
myfunction = getattr(mymodule, 'myfunction')

myfunction(parameter1, parameter2)

"""


@contextlib.contextmanager
def set_storage(object_type: str,
                object_name: str = "default",
                storage_parameter: dict = None,
                storage_format_instance: dict = None,
                subscription_level=1,
                _logger=None):

    action = {"localdisk":        {'1': pglocaldisk.PGLocaldisk(),
                                   '2': pglocaldisk.PGLocaldisk(),
                                   '999': pglocaldisk.PGLocaldisk(),
                                   },
              "s3":               {'1': pgs3.PGS3(),
                                   '2': pgs3.PGS3Ext(),
                                   '999': pgs3.PGS3Ext(),
                                   },
              }

    parameter = {"localdisk":     {'1': storage_parameter,
                                   '2': storage_parameter,
                                   '999': storage_parameter,
                                   },
                 "s3":            {'1': storage_parameter,
                                   '2': storage_parameter,
                                   '999': storage_parameter,
                                   },
                 }

    try:
        if object_type not in action:
            pggenericfunc.pg_error_logger(_logger,
                                          inspect.currentframe().f_code.co_name,
                                          f"{object_type} not found, currently {', '.join(action.keys())} are supported")
            raise

        instance_session = action.get(object_type, None)[str(subscription_level)]
        if instance_session and storage_parameter:
            instance_session.set_param(**parameter.get(object_type, None)[str(subscription_level)])

        for sf_inst in storage_format_instance.values():
            #for val in sf_inst.values():
                sf_inst._storage_instance[f"storage_{object_type}_{object_name}"] = instance_session

        _name = f"storage_{object_type}_{object_name}" if object_name else f"storage_{object_type}"
        instance_session.set_param(storage_format_instance=storage_format_instance,
                                   name={'name': _name})

        yield instance_session if instance_session else None

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)


def pgstorage(object_type: str,
              object_name: str = None,
              variable_name: str ="_pg_action",
              storage_parameter: dict = None,
              subscription_level: int = 1) -> Callable[[F], F]:

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = variable_name
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            #print(session_in_args)
            #print(session_in_kwargs)
            #print(kwargs)

            #if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name]
            #    if object_name is None or object_name in kwargs[variable_name][object_type]:

            if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name]:
                if object_name is None or object_name in kwargs[variable_name][object_type]:
                    return func(*args, **kwargs)
            else:
                _storage_format_instance = {}
                if variable_name in kwargs:
                    for _key in kwargs[variable_name].keys():
                        if _key.startswith("storage_format"):
                            _storage_format_instance[_key] = kwargs[variable_name][_key]
                #if arg_session in inspect.getfullargspec(func).args:

                with set_storage(object_type=object_type,
                                 object_name=object_name,
                                 storage_parameter=storage_parameter,
                                 storage_format_instance=_storage_format_instance,
                                 subscription_level=subscription_level) as session:
                    #print(session)
                    if session:
                        _object_name = object_name or "default"
                        if variable_name in kwargs:
                            #kwargs[variable_name][f"storage_{object_type}_{_object_name}"] = {_object_name: session}
                            kwargs[variable_name][f"storage_{object_type}_{_object_name}"] = session
                        else:
                            #object_name_namespace = {_object_name: session}
                            #kwargs[variable_name] = {f"storage_{object_type}_{_object_name}": object_name_namespace}

                            #kwargs[variable_name] = {f"storage_{object_type}_{_object_name}": session}
                            kwargs[variable_name] = {object_type: session}

                    print(kwargs)
                return func(*args, **kwargs)

        return wrapper
    return decorator

"""

@contextlib.contextmanager
def set_storage(storage_type: str,
                name: str = "default",
                storage_parameter: dict = None,
                storage_format_instance: dict = None,
                _logger=None):

    action = {"localdisk":        pglocaldisk.PGLocaldisk(),
              "s3":               pgs3.PGS3(),
              }

    parameter = {"localdisk":     {'storage_parameter': storage_parameter},
                 "s3":            {'storage_parameter': storage_parameter},
                 }

    try:
        if storage_type not in action:
            pggenericfunc.pg_error_logger(_logger,
                                          inspect.currentframe().f_code.co_name,
                                          f"storage type: {storage_type} is not found")
            raise

        instance = action[storage_type]
        if storage_parameter:
            instance.set_param(**parameter[storage_type])
            #pprint(instance.storage_parameter)

        for sf_inst in storage_format_instance.values():
            sf_inst._storage_instance[f"storage_{storage_type}_{name}"] = instance
        instance.set_param(storage_format_instance=storage_format_instance,
                           name=f"storage_{storage_type}_{name}")

        yield instance
    except Exception as err:
        raise Exception(err)

"""

"""
def pgstorage(storage_type: str, name: str = "default", storage_parameter: dict = None, _logger=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = f"storage_{storage_type}_{name}"
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                _storage_format_instance = {}
                for _key in kwargs.keys():
                    if _key.startswith("storage_format"):
                        _storage_format_instance[_key] = kwargs[_key]

                with set_storage(storage_type, name, storage_parameter, _storage_format_instance) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator
"""


"""
@contextlib.contextmanager
def set_storage(storage_type: str, storage_parameter: dict = None, storage_format_instance: list = None):

    action = {"localdisk":        pglocaldisk.PGLocaldisk(),
              "s3":               pgs3.PGS3(),
              }

    parameter = {"localdisk":     {'storage_parameter': storage_parameter},
                 "s3":            {'storage_parameter': storage_parameter},
                 }

    try:
        instance = action[storage_type]
        if storage_parameter:
            #print("storage paremeter:")
            #pprint(storage_parameter)
            instance.set_param(**parameter[storage_type])
            #pprint(instance.storage_parameter)


        if storage_format_instance:
            storage_format_instance.set_param(storage_instance=instance)
            instance.set_param(storage_format_instance=storage_format_instance)

        yield instance
    except Exception as err:
        raise Exception(err)

"""


if __name__ == '__main__':
    pass
