import pytest


class Checker:
    def start_monitoring(self, url):
        pass


class Storage:
    def list(self):
        return []


@pytest.fixture
def checker():
    return Checker()


@pytest.fixture
def storage():
    return Storage()