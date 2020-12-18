import json
import pytest
import psycopg2
import time
from psycopg2.extras import NamedTupleCursor
from subprocess import Popen
from os import environ


class Checker:
    def __init__(self, websites_file):
        self.websites_file = websites_file
        open(self.websites_file, 'w')

    def start_monitoring(self, url, page_regex=None):
        website = {'uri': url}
        if page_regex is not None:
            website['page_regex'] = page_regex
        websites_dict = [website]
        json.dump(websites_dict, open(self.websites_file, 'w'))


class Storage:
    def __init__(self, connstring):
        self.connstring = connstring
        self.truncate()

    def _execute(self, query, do_fetch=False):
        with psycopg2.connect(self.connstring) as connection:
            with connection.cursor(cursor_factory=NamedTupleCursor) as cursor:
                cursor.execute(query)
                if do_fetch:
                    return cursor.fetchall()

    def list(self):
        return self._execute('SELECT * FROM checker.list_results', do_fetch=True)

    def truncate(self):
        self._execute('TRUNCATE checker.results')


@pytest.fixture(scope='session')
def websites_file(tmpdir_factory):
    return str(tmpdir_factory.mktemp("websites").join("websites.json"))


@pytest.fixture(scope='session')
def producer(websites_file):
    producer_env = environ.copy()
    producer_env['WEBSITES_FILE'] = websites_file
    producer_process = Popen(['python', '-m', 'website_checker.producer'], env=producer_env)
    yield producer_process
    producer_process.kill()


@pytest.fixture(scope='session')
def consumer():
    consumer_process = Popen(['python', '-m', 'website_checker.consumer'])
    yield consumer_process
    consumer_process.kill()


@pytest.fixture
def checker(producer, consumer, websites_file):
    return Checker(websites_file)


@pytest.fixture
def storage():
    assert 'POSTGRES_URI' in environ
    return Storage(environ['POSTGRES_URI'])