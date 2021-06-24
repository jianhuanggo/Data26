import contextlib
import functools
import inspect
from Meta import pggenericfunc

def func(text):
    return text

@contextlib.contextmanager
def set_storage_format(storage_format: str, storage_instance=None, _logger=None):

    action = {'blockchain':       func,
              'excel':            func
              }

    try:
        yield action[storage_format]

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger,
                                      inspect.currentframe().f_code.co_name,
                                      err)
        raise


def pgstorageformat(name: str, storage_format: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            arg_session = f"{storage_format}_{name}"
            func_params = func.__code__.co_varnames
            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func(*args, **kwargs)
            else:
                if "storage" in kwargs:
                    _storage_instance = kwargs["storage"]
                else:
                    _storage_instance = None
                with set_storage_format(storage_format, _storage_instance) as session:
                    kwargs[arg_session] = session
                    return func(*args, **kwargs)

        return wrapper
    return decorator

@pgstorageformat("inst1", "blockchain")
@pgstorageformat("inst1", "blockchain")
def test1(*args, **kwargs):
    print(blockchain_inst1("looks good"))
    print("ok")


if __name__ == "__main__":
    test1()
