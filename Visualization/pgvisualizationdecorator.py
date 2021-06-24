"""
Exclusively for the purpose of managing daemon
"""

import contextlib
import functools
from Visualization.Pyplot import pgpyplot

from Daemon.Conf import dnconf


"""
mypackage = __import__('mypackage')
mymodule = getattr(mypackage, 'mymodule')
myfunction = getattr(mymodule, 'myfunction')

myfunction(parameter1, parameter2)

"""


@contextlib.contextmanager
def set_visualization(visualization_method: str):

    action = {"pyplot":            pgpyplot.PGPyplot().inst()
              }

    parameter = {"pyplot":         "reserved",
                 }

    try:
        instance = action[visualization_method]
        yield instance
    except Exception as err:
        raise Exception(err)


def visualize(visualization_method: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            arg_session = 'visualize'
            func_params = func.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                with set_visualization(visualization_method) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator


if __name__ == '__main__':
    pass
