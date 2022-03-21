from typing import Callable

def retry(fn: Callable, times=5):
    retry_counter = 0
    while True:
        try:
            fn()
            break
        except Exception as e:
            if retry_counter >= times:
                raise e
            retry_counter = retry_counter + 1
