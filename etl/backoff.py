import logging
import time
from functools import wraps
from elastic_transport import ConnectionError
import psycopg
import redis.exceptions

log = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while sleep_time < border_sleep_time:
                try:
                    return func(*args, **kwargs)

                except redis.exceptions.ConnectionError:
                    log.error(
                        "Произошла Ошибка при подключении к БД Redis. Ожидайте переподключения!"
                    )

                except psycopg.OperationalError:
                    log.error(
                        "Произошла Ошибка при подключении к БД PostgreSQL. Ожидайте переподключения!"
                    )

                except ConnectionError:
                    log.error(
                        "Произошла Ошибка при подключении к БД ElasticSearch. Ожидайте переподключения!"
                    )

                finally:
                    time.sleep(start_sleep_time)
                    sleep_time *= factor

        return inner

    return func_wrapper
