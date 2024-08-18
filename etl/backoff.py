import logging
import time
from functools import wraps
from typing import Any

import psycopg
import redis.exceptions
from elastic_transport import ConnectionError

log = logging.getLogger(__name__)


def backoff(start_sleep_time: int = 0.1, factor: int = 2, border_sleep_time: int = 10) -> Any:
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
            errors_count = 0
            max_count = 7
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

                time.sleep(start_sleep_time)
                sleep_time *= factor
                errors_count += 1

                if errors_count == max_count:
                    break

        return inner

    return func_wrapper
