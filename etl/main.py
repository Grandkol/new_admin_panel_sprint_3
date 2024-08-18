import logging
import os
import time

import psycopg
from backoff import backoff
from dotenv import load_dotenv
from extract import Pg_Extractor
from load import ElasticLoad
from psycopg import ClientCursor, connection
from psycopg.rows import dict_row
from transform import TransformToElastic
from config import settings

log = logging.getLogger(__name__)

TABLES = ["film_work", "person", "genre"]

load_dotenv()


def start_etl(pg_conn: connection) -> None:
    entry = Pg_Extractor(tables=TABLES, pg_conn=pg_conn)
    data = entry.extraction_logic()

    transform = TransformToElastic(data=data)
    docs = transform.transform()

    load = ElasticLoad(data=docs)
    load.load()


# @backoff()
def main(pg_conn: connection) -> None:
    while True:
        start_etl(pg_conn=pg_conn)
        time.sleep(5)


if __name__ == "__main__":
    dsl = {
        "dbname": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
        "host": settings.db_host,
        "port": settings.db_port,
    }
    pg_conn = psycopg.connect(**dsl, row_factory=dict_row, cursor_factory=ClientCursor)
    main(pg_conn=pg_conn)
