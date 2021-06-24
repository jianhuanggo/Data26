from typing import TypeVar, Callable, Any, Generic
import functools

F = TypeVar('F', bound=Callable[..., Any])


class copy_signature(Generic[F]):
    def __init__(self, target: F) -> None: ...
    def __call__(self, wrapped: Callable[..., Any]) -> F: ...


def f(redfin, *extra: int) -> str:
    return "this is a test"


def api_real_estate(api_name):
    def decorator(func1):
        @functools.wraps(func1)
        def wrapper(*args, **kwargs):

            arg_session = api_name
            func_params = func1.__code__.co_varnames

            session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
            session_in_kwargs = arg_session in kwargs

            if session_in_kwargs or session_in_args:
                return func1(*args, **kwargs)
            else:
                    kwargs[arg_session] = "this is a test"
                    #create_a_function(name=f"pg_api_{api_name}")
                    func = test
                    return func(*args, **kwargs)

        return wrapper
    return decorator



@api_real_estate("redfin")
@copy_signature(f)
def test(*args, **kwargs):
    print("this is a test")


#reveal_type(test)

if __name__ == "__main__":
    test()
