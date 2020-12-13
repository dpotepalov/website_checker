import pytest
import psycopg2
from subprocess import Popen
from os import environ


class Checker:
    def start_monitoring(self, url):
        pass


class Storage:
    def __init__(self, connstring):
        self.connstring = connstring

    def _execute(self, query):
        with psycopg2.connect(self.connstring) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    def list(self):
        return self._execute('SELECT * FROM checker.list_results')


@pytest.fixture(scope='session')
def producer():
    producer_process = Popen(['python', '-m', 'website_checker.producer'])
    yield producer_process
    producer_process.kill()


@pytest.fixture(scope='session')
def consumer():
    consumer_process = Popen(['python', '-m', 'website_checker.consumer'])
    yield consumer_process
    consumer_process.kill()


@pytest.fixture
def checker(producer, consumer):
    return Checker()


@pytest.fixture
def storage():
    assert 'STORAGE_CONNSTRING' in environ
    return Storage(environ['STORAGE_CONNSTRING'])