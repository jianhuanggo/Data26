from API.RealEstate import pgrealestate
import inspect
from typing import TypeVar, Callable, Any, Generic


def create_a_function(func, *args, **kwargs):
    def function_template(*args, **kwargs):
        func(*args, **kwargs)
    return function_template


def function_details(func):
    # Getting the argument names of the
    # called function
    argnames = func.__code__.co_varnames[:func.__code__.co_argcount]

    # Getting the Function name of the
    # called function
    fname = func.__name__

    def inner_func(*args, **kwargs):
        print(fname, "(", end="")

        # printing the function arguments
        print(', '.join('% s = % r' % entry
                        for entry in zip(argnames, args[:len(argnames)])), end=", ")

        # Printing the variable length Arguments
        print("args =", list(args[len(argnames):]), end=", ")

        # Printing the variable length keyword
        # arguments
        print("kwargs =", kwargs, end="")
        print(")")

    return inner_func


F = TypeVar('F', bound=Callable[..., Any])


class copy_signature(Generic[F]):
    def __init__(self, target: F) -> None: ...
    def __call__(self, wrapped: Callable[..., Any]) -> F: ...


def f(x: bool, *extra: int) -> str:
    return "this is a test"


@copy_signature(f)
def test(*args, **kwargs):
    return f(*args, **kwargs)



#@function_details
@pgrealestate.api_real_estate("redfin")
def test10(*args, **kwargs):
    print("this is a test")
    print(kwargs['redfin'])




if __name__ == '__main__':
    #test10()

    #a = inspect.signature(test)
    #b = inspect.signature(f)
    print(a)
    print(b)
