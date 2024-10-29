from os import path, environ
import time

import psycopg2
from threading import Thread


SERVICE_DIR = path.dirname(path.realpath(__file__))
HOME_DIR = SERVICE_DIR.rsplit(path.sep, 1)[0]


PULL_ZONE_ROOT = environ["BUNNY_PULL_ZONE_ROOT"]
LIBRARY_CDN_HOSTNAME = environ["LIBRARY_CDN_HOSTNAME"]

class Database:
    def __init__(self) -> None:
        self.postgres_connection: None | psycopg2.extensions.connection = None
        self._create_connection()

        self.connection_retry_delay: int = 5 # Seconds
        self.connected = self._check_connection_to_postgres()

        self.connection_status_poller_thread = Thread(
            target=self._poller_target, args=(), daemon=True, name="connection_status_poller_thread"
        ).start()

    def _poller_target(self):
        while True:
            time.sleep(5)
            self._poll_connection()

    def _poll_connection(self):
        self.connected = self._check_connection_to_postgres() 

        while not self.connected:
            time.sleep(self.connection_retry_delay)
            self._create_connection()
            self.connected = self._check_connection_to_postgres() 

    def _check_connection_to_postgres(self):
        '''Returns True if the connection is active and usable. Returns False otherwise.'''
        connection = self.postgres_connection
        if connection.closed == 0:
            return True
        return False

    def _create_connection(self) -> None:
        self.postgres_connection = psycopg2.connect(
            database=environ["POSTGRESDB_DATABASE"],
            host=environ["POSTGRESDB_HOST"],
            user=environ["POSTGRESDB_USER"],
            password=environ["POSTGRES_PASSWORD"],
            port=environ["POSTGRESDB_DOCKER_PORT"]
        )
        self.postgres_connection.set_session(autocommit=True) # Very important for our use case!

    def execute_sql_query(self, query_string: str, args: tuple = None) -> psycopg2.extensions.cursor:
        '''Returns a postgres cursor after executing the provided `query_string`.'''
        pg_cursor = self.postgres_connection.cursor()
        if args is not None:
            pg_cursor.execute(query_string, args)
        else:
            pg_cursor.execute(query_string)
        return pg_cursor