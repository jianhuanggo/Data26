"""
Exclusively for the purpose of managing daemon
"""
import os
import json
import inspect
import functools
import contextlib
import pandas as pd
from types import SimpleNamespace
from typing import Callable, Any, TypeVar


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




_file_ext = pg_data.split('.')[-1]
if _file_ext in ("yml", "yaml"):
    self._data_inputs = [(key, item) for key, item in
                         pgyaml.yaml_load(yaml_filename=pg_data).items()]
elif _file_ext in (".csv"):
    if "file_delimiter" in self._parameter:
        _df = pd.read_csv(pg_data, sep=self._parameter["file_delimiter"])
    else:
        # print(pg_data)
        _df = pd.read_csv(pg_data, sep=',')
        if len(_df) < 100:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                          "not enough data to train the models")
            raise
"""


def pg_sf_validate(file_info: dict) -> bool:
    _pg_sanity_check = {"0": True if file_info.get("content_size", 0) >= .1 * file_info.get("file_size", 0) else False
    }
    for _pg_sc_num, _pg_sc_logic in _pg_sanity_check.items():
        if not _pg_sc_logic:
            print(f"file format sanity check #{_pg_sc_num} failed, data is probably not good")
            return False
    return True


def pg_sf_size(pg_data) -> int:
    if isinstance(pg_data, pd.DataFrame):
        return pg_data.size
    else:
        return len(pg_data) if pg_data else 0


def pg_md_autofill_df(pg_data: pd.DataFrame, pg_method: str = "drop"):
    #pg_data.isna()
    #pg_data.isnull().sum()
    #pg_data.notna()
    #pg_data.notnull()
    #pg_data[pg_data.isnull().any(axis=1)]
    #
    pg_data.dropna(inplace=True) if pg_method == "drop" else pg_data.fillna(pg_data.mean(), inplace=True)
    return pg_data


def pg_md_autofill_default(pg_data):
    return pg_data


def pg_missing_data_autofill(pg_data, pg_sf, _logger=None):
    _storage_format = {'csv': pg_md_autofill_df,
                       'json': pg_md_autofill_default,
                       'yaml': pg_md_autofill_default
                       }
    try:
        return _storage_format.get(pg_sf, None)(pg_data) if _storage_format.get(pg_sf, None) else None

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)
    return None


@contextlib.contextmanager
def pg_set_storage_format(pg_files, _logger=None):
    _storage_format = {'csv': pd.read_csv,
                       'json': json.loads,
                       'yaml': pgyaml.yaml_load
    }

    if isinstance(pg_files, dict):
        pg_files = [x for y in [val for _, val in pg_files.items()] for x in y]
    elif isinstance(pg_files, str):
        pg_files = [pg_files]
    elif isinstance(pg_files, list):
        pass
    else:
        print("Unsupport file format")

    for _pg_file in pg_files:
        try:
            _file_ext = _pg_file.split('.')[-1]
            _pg_data = _storage_format.get(_file_ext, None)(_pg_file)

            _pg_data = pg_missing_data_autofill(_pg_data, _file_ext)

            print(_pg_data)
            #print("aaaaaa1111")
            #print(type(_pg_data))
            #print(pg_sf_size(_pg_data))
            #exit(0)

            if _pg_data is not None and pg_sf_validate({"file_size": os.path.getsize(_pg_file),
                                                        "content_size": pg_sf_size(_pg_data)}):
                yield {"file_format": _file_ext,
                       "data": _pg_data
                       }
            else:
                pggenericfunc.pg_error_logger(_logger,
                                              inspect.currentframe().f_code.co_name,
                                              "file format detection by file extension is unsuccessful")
        except Exception as err:
            pggenericfunc.pg_error_logger(_logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)
        #exit(0)
        for key, item in _storage_format.items():
            try:
                _pg_data = item(_pg_file)
                if pg_sf_validate({"file_size": os.path.getsize(_pg_file), "content_size": pg_sf_size(_pg_data)}):
                    yield {"file_format": key,
                           "data": pg_missing_data_autofill(_pg_data, key)
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
