from contextlib import contextmanager
from datetime import datetime
from os import environ

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as postgres_connection
from psycopg2.extras import DictCursor
from storage import State

load_dotenv()
dsl = {
    'dbname': environ.get('POSTGRES_DB'),
    'user': environ.get('POSTGRES_USER'),
    'password': environ.get('POSTGRES_PASSWORD'),
    'host': environ.get('POSTGRES_HOST'),
    'port': environ.get('POSTGRES_PORT'),
    'options': '-c search_path=content',
}

CHUNK_SIZE = int(environ.get('CHUNK_SIZE'))


@contextmanager
def open_postgresql_db(dsl: dict):
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()


class PostgresExtractor:
    def __init__(self, connection: postgres_connection, query: str, state: State) -> None:
        self.conn = connection
        self.cursor = self.conn.cursor()
        self.query = query
        self.state = state
        self.batch_size = CHUNK_SIZE

    def extract(self):
        query, last_extracted = self.get_query()
        self.cursor.execute(query, (last_extracted,))
        while True:
            data = self.cursor.fetchmany(CHUNK_SIZE)
            if data:
                yield data
            else:
                break

    def get_query(self):
        last_extracted = self.state.get_state('last_checked')
        if last_extracted is None:
            last_extracted = datetime.now()
            return self.query % '< %s', last_extracted.strftime('%Y-%m-%d %H:%M:%S')
        return self.query % '> %s', last_extracted
