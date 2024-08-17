import logging
import os
from typing import Any



import redis
from dotenv import load_dotenv


log = logging.getLogger(__name__)

load_dotenv()


class State:
    """Класс для работы с состояниями."""

    def __init__(self) -> None:
        self.storage = redis.Redis(host=os.getenv("REDIS_HOST"),
                                   port=os.getenv("REDIS_PORT"))
        print(self.storage.scan())

    def empty_storage(self):
        self.storage.flushdb()

    def set_state(self, key, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        return self.storage.set(key, f"{value}")

    def get_state(self, key) -> Any:
        if self.storage.get("person_last_date") is None:
            self.storage.set("person_last_date", "01-01-0001")
        if self.storage.get("movies_last_date") is None:
            self.storage.set("movies_last_date", "01-01-0001")
        if self.storage.get("genre_last_date") is None:
            self.storage.set("genre_last_date", "01-01-0001")
        """Получить состояние по определённому ключу."""
        return self.storage.get(key).decode("utf-8")


class Pg_Extractor:

    def __init__(self, tables, pg_conn):

        self.curs_pg = pg_conn.cursor()
        self.tables = tables
        self.state = State()

    def extraction_logic(self):
        for table in self.tables:
            if table == "film_work":
                ids, mod = self.person_ids(
                    table=table, state=self.state.get_state("movies_last_date")
                )
                if ids == ():
                    return []
                # last_date = mod[-1]
                self.state.set_state("movies_last_date", mod[-1])
                return self.film_information(ids=ids)

            if table == "person":
                ids, mod = self.person_ids(
                    table=table, state=self.state.get_state("person_last_date")
                )
                if ids == ():
                    return []

                self.state.set_state("person_last_date", mod[-1])
                film_ids = self.film_ids(table=table, ids=ids)
                return self.film_information(ids=film_ids)

            if table == "genre":
                ids, mod = self.person_ids(
                    table=table, state=self.state.get_state("genre_last_date")
                )
                if ids == ():
                    return []

                self.state.set_state("person_last_date", mod[-1])
                film_ids = self.film_ids(table=table, ids=ids)
                return self.film_information(ids=film_ids)

    def person_ids(self, table, state):
        list_id = []
        list_mod = []

        # try:
        self.curs_pg.execute(
            "SELECT id, modified "
            f"FROM content.{table} "
            f"WHERE modified > '{state}' "
            "ORDER BY modified "
            "LIMIT 100; "
        )
        for table in self.curs_pg.fetchall():
            ids, modified = str(table["id"]), table["modified"]
            list_id.append(ids)
            list_mod.append(modified)

        list_id = tuple(list_id)
        list_mod = tuple(list_mod)

        return list_id, list_mod

    def film_ids(self, table, ids):
        list_id = []
        list_mod = []

        self.curs_pg.execute(
            f"SELECT fw.id, fw.modified "
            f"FROM content.film_work fw "
            f"LEFT JOIN content.{table}_film_work pfw "
            f"ON pfw.film_work_id = fw.id "
            f"WHERE pfw.person_id "
            f"IN {ids} "
            f"ORDER BY fw.modified "
            f"LIMIT 100; "
        )

        for table in self.curs_pg.fetchall():
            ids, modified = str(table["id"]), table["modified"]
            list_id.append(ids)
            list_mod.append(modified)
        list_id = tuple(list_id)
        return list_id

    def film_information(self, ids):

        self.curs_pg.execute(
            "SELECT "
            "fw.id, "
            "fw.title, "
            "fw.description, "
            "fw.rating, "
            "fw.type, "
            "fw.created,"
            "fw.modified, "
            "array_agg(DISTINCT g.name) AS genres,"
            "COALESCE( "
            "json_agg(DISTINCT jsonb_build_object( "
            "'person_role', pfw.role, "
            "'person_id', p.id, "
            "'person_name', p.full_name)) "
            "FILTER (WHERE p.id IS NOT NULL),"
            "'[]'"
            ") AS persons "
            "FROM content.film_work fw "
            "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            "LEFT JOIN content.person p ON p.id = pfw.person_id "
            "LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
            f" WHERE fw.id IN {ids} "
            "GROUP BY fw.id "
            "ORDER BY fw.modified; "
        )

        tables = self.curs_pg.fetchall()
        return tables

