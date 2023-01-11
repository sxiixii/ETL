import time
from datetime import datetime
from os import environ

from dotenv import load_dotenv
from etl_logger import log
from psycopg2.extensions import connection as postgres_connection
from storage import JsonFileStorage, State

from .extractor import PostgresExtractor, dsl, open_postgresql_db, tables
from .extractor.utilities import pg_backoff
from .loader import ESLoader, create_index, es_index_is_empty
from .loader.es_index import index as es_index
from .loader.utilities import es_backoff
from .transformer import Transformer

load_dotenv()

ES_INDEX = environ.get('ES_INDEX')
TIME_TO_SLEEP = int(environ.get('TIME_TO_SLEEP'))


@pg_backoff(start_sleep_time=2, factor=2, border_sleep_time=10)
def extract(connect: postgres_connection, name: str, state: State):
    extractor = PostgresExtractor(connect, tables[name], state)
    return extractor.extract()


def transform(data: list[list]):
    transformer = Transformer(data)
    return transformer.transform_data()


@es_backoff(start_sleep_time=2, factor=2, border_sleep_time=10)
def load(data: list[dict], state: State, index: str):
    loader = ESLoader(data, state, index)
    return loader.load_data_to_es()


def get_state(file_name: str) -> State:
    storage = JsonFileStorage(file_name)
    return State(storage)


def save_state(extracted: str,) -> None:
    for table_name in tables.keys():
        state = get_state(table_name)
        state.set_state('last_extracted', extracted)


def run_pipeline(connect: postgres_connection, name: str, state: State) -> None:
    load(transform(extract(connect, name, state)), state, ES_INDEX)


def run_init_pipeline(connect: postgres_connection, table: str) -> None:
    init_state = get_state(table)
    run_pipeline(connect, table, init_state)
    last_extracted = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    save_state(last_extracted)


if __name__ == '__main__':
    create_index('movies', es_index)
    with open_postgresql_db(dsl) as conn:
        if es_index_is_empty(ES_INDEX):
            run_init_pipeline(conn, 'films')
        while True:
            for table_name, query in tables.items():
                state = get_state(table_name)
                run_pipeline(conn, table_name, state)
                log.info(f'Run pipelines for "{table_name}" table')
                time.sleep(TIME_TO_SLEEP)
            time.sleep(TIME_TO_SLEEP)
