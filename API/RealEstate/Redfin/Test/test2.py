import functools
import inspect
from typing import TypeVar, Callable, Any, Generic

F = TypeVar('F', bound=Callable[..., Any])


class copy_signature(Generic[F]):
    def __init__(self, target: F) -> None:
        print(F)

    def __call__(self, wrapped: Callable[..., Any]) -> F:
        return wrapped


def test10():
    print("ok1111")


def test(a, b):
    return a + b


def add_variable(name: str):

    def decorator(func):
        print(name)
        arg_session = "try"
        @functools.wraps(func)
        #@copy_signature(func)
        def wrapper(try11, *args, **kwargs):

            return func(11, *args, **kwargs)
        return wrapper
    return decorator


test2 = add_variable("me")(test)

#test3 = copy_signature(test)(test10)

test4 = functools.partial(test)

if __name__ == '__main__':
    a = inspect.signature(test)
    b = inspect.signature(test2)
    #c = inspect.signature(test3)
    print(a)
    print(b)
    #print(c)
    print(test(10, 20))
    print(test2(10, 20))
    #print(test3(10, 20))

