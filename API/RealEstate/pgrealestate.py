import inspect
import contextlib
import functools
from types import SimpleNamespace
from typing import Callable, Any, TypeVar
from Meta import pggenericfunc
from API.RealEstate.Redfin import pgredfin
from Data.Config import pgconfig

__version__ = "1.7"

F = TypeVar('F', bound=Callable[..., Any])

def create_a_function(func, name, *args, **kwargs):
    def function_template(*args, **kwargs):
        func(*args, **kwargs)
    return function_template

#my_new_function = create_a_function()



@contextlib.contextmanager
def create_session(object_type, subscription_level: int = 1, logger=None):
    #_conf = pgconfig.Config()

    action = {"redfin":            {'1': pgredfin.PGRedfin(),
                                    '2': pgredfin.PGRedfinExt(),
                                    '999': pgredfin.PGRedfinSingleton(),
                                    },

              'realtor.com':        {'1': pgredfin.PGRedfin(),
                                     '2': pgredfin.PGRedfinExt(),
                                     '999': pgredfin.PGRedfinSingleton(),
                                    },
              'zillow':             {'1': pgredfin.PGRedfin(),
                                     '2': pgredfin.PGRedfinExt(),
                                     '999': pgredfin.PGRedfinSingleton(),

                                    },
              }

    parameter = {'redfin':         pgredfin.PGRedfin(),
                 'realtor.com':    pggenericfunc.notimplemented,
                 'zillow':         pggenericfunc.notimplemented
                 }

    #Not_found = f"{api_name} not found, currently {' '.join(action.keys())} are supported"

    try:
        if object_type not in action:
            print(f"{object_type} not found, currently {', '.join(action.keys())} are supported")

            #instance = action[db_name](parameter[db_name](db_name))
        api_session = action.get(object_type, None)[str(subscription_level)]
        if api_session:
            api_session.set_param(**{'name': object_type})
        yield api_session if api_session else None

    except Exception as err:
        pggenericfunc.pg_error_logger(logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)


def api_real_estate(object_type: str, object_name: str, variable_name: str ="_pg_action", subscription_level: int = 1) -> Callable[[F], F]:

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = variable_name
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            print(session_in_args)
            print(session_in_kwargs)
            print(kwargs)

            # if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[variable_name]
            #    if object_name is None or object_name in kwargs[variable_name][object_type]:

            if (session_in_kwargs or session_in_args) and variable_name in kwargs and object_type in kwargs[
                variable_name].__dict__:
                if object_name is None or object_name in kwargs[variable_name].__dict__[object_type].__dict__:
                    return func(*args, **kwargs)
            else:
                #if arg_session in inspect.getfullargspec(func).args:
                with create_session(object_type, subscription_level) as session:
                    print(session)
                    if session:
                        if object_name:
                            #object_name_namespace = {object_name: session}
                            #kwargs[variable_name] = {object_type: object_name_namespace}
                            object_name_namespace = SimpleNamespace(**{object_name: session})
                            kwargs[variable_name] = SimpleNamespace(**{object_type: object_name_namespace})
                        else:
                            kwargs[variable_name] = {object_type: session}
                    print(kwargs)
                return func(*args, **kwargs)

        return wrapper
    return decorator

"""
def api_real_estate(api_name: str , variable_name: str ="_pg_action"):

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = variable_name
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if (session_in_kwargs or session_in_args) and api_name in kwargs[variable_name]:
                return func(*args, **kwargs)
            else:
                with create_session(api_name) as session:
                    print(session)
                    if session:
                        if variable_name not in kwargs:
                            kwargs[variable_name] = {}
                        kwargs[variable_name][api_name] = session
                        print(kwargs)
                    return func(*args, **kwargs)

        return wrapper
    return decorator


def api_real_estate(variable_name, api_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = api_name
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with create_session(api_name) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator
    """