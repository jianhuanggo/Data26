import timeit
import functools


def timer(number, repeat):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print(locals())
            runs = timeit.repeat(lambda: func(*args), number=number, repeat=repeat)
            print(sum(runs) / len(runs))
        return wrapper
    return decorator
