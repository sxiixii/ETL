import time
from datetime import datetime
from os import environ

from dotenv import load_dotenv
from etl_logger import log
from psycopg2.extensions import connection as postgres_connection
from storage import JsonFileStorage, State

from .extractor import PostgresExtractor, dsl, open_postgresql_db, INDEX_AND_TABLES
from .extractor.utilities import pg_backoff
from .loader import ESLoader, create_index, es_index_is_empty
from .loader.es_index import movies_index, persons_index
from .loader.utilities import es_backoff
from .transformer import FilmTransformer, PersonTransformer

load_dotenv()

FILM_ES_INDEX = environ.get('FILM_ES_INDEX')
PERSON_ES_INDEX = environ.get('PERSON_ES_INDEX')
GENRE_ES_INDEX = environ.get('GENRE_ES_INDEX')

TRANSFORMER_FOR_INDEX = {
    FILM_ES_INDEX: FilmTransformer,
    PERSON_ES_INDEX: PersonTransformer
}

TIME_TO_SLEEP = int(environ.get('TIME_TO_SLEEP'))


@pg_backoff(start_sleep_time=2, factor=2, border_sleep_time=10)
def extract(connect: postgres_connection, query: str, state: State):
    extractor = PostgresExtractor(connect, query, state)
    return extractor.extract()


def transform(data: list[list], transformer):
    transformer = transformer(data)
    return transformer.transform_data()


@es_backoff(start_sleep_time=2, factor=2, border_sleep_time=10)
def load(data: list[dict], state: State, index: str):
    loader = ESLoader(data, state, index)
    return loader.load_data_to_es()


def get_state(file_name: str) -> State:
    storage = JsonFileStorage(file_name)
    return State(storage)


def save_state(extracted: str, state_name) -> None:
    state = get_state(state_name)
    state.set_state('last_checked', extracted)


def run_pipeline(connection, es_index):
    for table_name, query in INDEX_AND_TABLES[es_index].items():
        state_name = es_index + '_' + table_name
        state = get_state(state_name)

        items_chunk = extract(connection, query, state)
        transformed_items_chunk = transform(items_chunk, TRANSFORMER_FOR_INDEX[es_index])
        load(transformed_items_chunk, state, es_index)
        last_extracted = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        save_state(last_extracted, state_name)

        log.info(f'Run pipelines for "{state_name}" table')
        time.sleep(TIME_TO_SLEEP)


if __name__ == '__main__':
    time.sleep(20)
    create_index('movies', movies_index)
    create_index('persons', persons_index)
    with open_postgresql_db(dsl) as conn:
        while True:
            for index in INDEX_AND_TABLES.keys():
                run_pipeline(conn, index)
                time.sleep(TIME_TO_SLEEP)
