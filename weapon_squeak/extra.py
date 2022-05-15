from __future__ import annotations
import typing
from time import time
from functools import partial
__all__ = [
    "get_epoch"
    , "split_hostport"
    , "Singleton"
]



get_epoch: typing.Callable[[], int] = lambda: int( time() )
split_hostport = partial(
    map
    , lambda x: x.split(":")
)


# https://stackoverflow.com/a/6798042
class Singleton (type):
    _instances = {}
    def __call__ (cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super( Singleton, cls ).__call__( *args, **kwargs )
        return cls._instances[cls]
