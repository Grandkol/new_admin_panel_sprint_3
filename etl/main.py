import logging
import os
import time
from threading import Timer
import psycopg
import psycopg2.extras
import redis
from dotenv import load_dotenv
from psycopg import ClientCursor, connection as _connection
from psycopg import sql
from psycopg.rows import dict_row

from extract import Pg_Extractor, State
from transform import TransformToElastic
from load import ElasticLoad
from backoff import backoff

log = logging.getLogger(__name__)

TABLES = ["film_work", "person", "genre"]

load_dotenv()


@backoff()
def start_etl(pg_conn):
    try:
        entry = Pg_Extractor(tables=TABLES, pg_conn=pg_conn)
        data = entry.extraction_logic()

        transform = TransformToElastic(data=data)
        docs = transform.transform()

        load = ElasticLoad(data=docs)
        load.load()
    finally:
        pg_conn.close()


def main(pg_conn):
    while True:
        start_etl(pg_conn=pg_conn)
        time.sleep(10)


if __name__ == "__main__":
    dsl = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("SQL_HOST"),
        "port": os.getenv("SQL_PORT"),
    }
    pg_conn = psycopg.connect(**dsl, row_factory=dict_row, cursor_factory=ClientCursor)


    main(pg_conn=pg_conn)
