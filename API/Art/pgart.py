import inspect
import contextlib
import functools
from types import SimpleNamespace
from typing import Callable, Any, TypeVar
from Meta import pggenericfunc


__version__ = "1.8"

F = TypeVar('F', bound=Callable[..., Any])


@contextlib.contextmanager
def create_session(object_type, subscription_level=1, logger=None):
    #_conf = pgconfig.Config()

    # action = {"pandasdatareader": {'1': pgpandasreader.PGPandasDataReader(),
    #                                '2': pgpandasreader.PGPandasDataReaderExt(),
    #                                '999': pgpandasreader.PGPandasDataReaderSingleton(),
    #                                },
    #           }
    #
    # parameter = {'pandasdatareader':  {'1': pgpandasreader.PGPandasDataReader(),
    #                                    '2': pgpandasreader.PGPandasDataReaderExt(),
    #                                    '999': pgpandasreader.PGPandasDataReaderSingleton(),
    #                                   },
    #              }

    action = {}
    parameter = {}

    try:
        if object_type not in action:
            print(f"{object_type} not found, currently {', '.join(action.keys())} are supported")

        api_session = action.get(object_type, None)[str(subscription_level)]
        if api_session:
            api_session.set_param(**{'name': object_type})
        yield api_session if api_session else None

    except Exception as err:
        pggenericfunc.pg_error_logger(logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)


def api_art(object_type: str, object_name: str = None, variable_name: str ="_pg_action", subscription_level: int = 1) -> Callable[[F], F]:

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

            if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name].__dict__:
                if object_name is None or object_name in kwargs[variable_name].__dict__[object_type].__dict__:
                    return func(*args, **kwargs)
            else:
                #if arg_session in inspect.getfullargspec(func).args:
                with create_session(object_type, subscription_level) as session:
                    #print(session)
                    if session:
                        if object_name:
                            #object_name_namespace = {object_name: session}
                            #kwargs[variable_name] = {object_type: object_name_namespace}
                            object_name_namespace = SimpleNamespace(**{object_name: session})
                            kwargs[variable_name] = SimpleNamespace(**{object_type: object_name_namespace})
                        else:
                            kwargs[variable_name] = {object_type: session}
                    #print(kwargs)
                return func(*args, **kwargs)

        return wrapper
    return decorator