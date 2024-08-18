import logging
from typing import Dict

from backoff import backoff
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from config import settings

load_dotenv()

log = logging.getLogger(__name__)


class ElasticLoad:
    def __init__(self, data):

        self.client = Elasticsearch(settings.elastic_host)
        self.data = data

    @backoff
    def load(self) -> None:
        if self.data == []:
            log.info("Нет Данных для загрузки")
            return

        def get_doc_data() -> Dict:
            for doc in self.data:
                yield {"_id": doc["id"], "_index": "movies", "_source": doc}

        bulk(self.client, get_doc_data())
        log.info("Загрузка в ES прошла успешно!")
