import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)


class ElasticLoad:
    def __init__(self, data):

        self.client = Elasticsearch(os.getenv("ELASTIC_HOST"))
        self.data = data

    def load(self):
        if self.data == []:
            print("Нет Данных для загрузки")
            return

        def get_doc_data():
            for doc in self.data:
                yield {"_id": doc["id"], "_index": "movies", "_source": doc}

        bulk(self.client, get_doc_data())
        log.info("Загрузка в ES прошла успешно!")
