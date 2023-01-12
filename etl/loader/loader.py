from datetime import datetime
from os import environ

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from etl_logger import log
from storage import State

from .utilities import es_backoff

load_dotenv()

CHUNK_SIZE = int(environ.get('CHUNK_SIZE'))

es = Elasticsearch(environ.get('ES_HOST'))


@es_backoff(start_sleep_time=0.5, factor=2, border_sleep_time=10)
def create_index(index_name: str, index_body: str):
    if not es.indices.exists(index=index_name):
        response = es.indices.create(index=index_name, body=index_body)
        log.info('Create index: ', response)


@es_backoff(start_sleep_time=0.5, factor=2, border_sleep_time=10)
def es_index_is_empty(index: str) -> bool:
    response = es.search(index=index)
    loaded_items = response['hits']['total']['value']
    return True if loaded_items == 0 else False


class ESLoader:
    def __init__(self, data: list[dict], state: State, index: str):
        self.data = data
        self.state = state
        self.index = index

    def es_bulk_data_generator(self):
        for chunk in self.data:
            for item in chunk:
                es_index = {"_index": self.index, "_id": item["id"]}
                es_index.update(item)
                yield es_index

    def load_data_to_es(self):
        if self.data:
            for ok, info in streaming_bulk(es, self.es_bulk_data_generator(), chunk_size=CHUNK_SIZE):
                if not ok:
                    log.error('An error occurred while loading: ', info)
                else:
                    last_extracted = datetime.now()
                    self.state.set_state('last_checked', last_extracted.strftime('%Y-%m-%d %H:%M:%S'))
