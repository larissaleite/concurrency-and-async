import functools
from time import time


def duration(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        func()
        end = time()
        print(f"Total duration of {func.__name__}: {end - start} seconds.")

    return wrapper
