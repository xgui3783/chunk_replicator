from typing import Callable
from functools import wraps
from time import sleep

from .exceptions import RetryFailedException

def retry(fn: Callable, times=5):
    retry_counter = 0
    while True:
        try:
            return fn()
        except Exception as e:
            if retry_counter >= times:
                raise e
            retry_counter = retry_counter + 1

def retry_dec(times=5, wait=1):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            exceptions = []
            counter=0
            while counter < times:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    exceptions.append(e)
                    counter = counter + 1
                    sleep(wait)
            raise RetryFailedException(exceptions)
            
        return inner
    return outer