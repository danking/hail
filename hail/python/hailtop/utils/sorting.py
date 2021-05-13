from typing import Tuple, Callable, Optional


# No types because mypy cannot produce rank-1 polymorphic functions
def none_last(key):
    if key:
        return lambda x: (x is None, key(x))
    return lambda x: (x is None, x)


def none_first(key):
    if key:
        return lambda x: (x is not None, key(x))
    return lambda x: (x is not None, x)
