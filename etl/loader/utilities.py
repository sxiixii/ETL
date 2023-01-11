import time
from functools import wraps

from elastic_transport import TransportError
from etl_logger import log


def es_backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                value = func(*args, **kwargs)
                return value
            except TransportError:
                log.error('Connection lost. Trying to connect...')
                time_to_sleep = start_sleep_time
                is_connected = False
                n = 1
                while not is_connected:
                    try:
                        time.sleep(time_to_sleep)
                        value = func(*args, **kwargs)
                        is_connected = True
                        log.debug('Connected to ES!')
                        return value
                    except TransportError:
                        log.error(f'Connection lost. Trying to connect a {n} time.')
                        time_to_sleep = start_sleep_time * factor**n
                        if time_to_sleep > border_sleep_time:
                            time_to_sleep = border_sleep_time
                        n += 1
        return inner
    return func_wrapper
