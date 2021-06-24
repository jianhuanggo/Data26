import inspect
import contextlib
import functools
from types import SimpleNamespace
from typing import Callable, Any, TypeVar
from Meta import pggenericfunc
from API.News.Newscatcher import pgnewscatcher
from Data.Config import pgconfig

__version__ = "1.7"

F = TypeVar('F', bound=Callable[..., Any])


@contextlib.contextmanager
def create_session(object_type, subscription_level=1, logger=None):
    #_conf = pgconfig.Config()

    action = {"newscatcher":     {'1': pgnewscatcher.PGNewsCatcher(),
                                  '2': pgnewscatcher.PGNewsCatcherExt(),
                                  '999': pgnewscatcher.PGNewsCatcherSingleton(),
                                  },
              }

    parameter = {'newscatcher':  {'1': pgnewscatcher.PGNewsCatcher(),
                                  '2': pgnewscatcher.PGNewsCatcherExt(),
                                  '999': pgnewscatcher.PGNewsCatcherSingleton(),
                                  },
                 }

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


def api_news(object_type: str, object_name: str = None, variable_name: str = "_pg_action", subscription_level: int = 1) -> Callable[[F], F]:
    """
    decorate a function and attach instance of api_news to the function's variable specified in variable_name.
    :parameter
        :param object_type: string - object type
        :param object_name: string - object name
        :param variable_name: string - variable name which instance will be associated to
        :param subscription_level: integer - subscription level
    :return
        return modified function
    """

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

