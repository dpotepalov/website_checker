from datetime import timedelta, datetime
from time import sleep
from website_checker.structs import CheckResult


def _ms_from_now(x):
    return datetime.now() + timedelta(milliseconds=x)


def _wait_until_changes(func, until, unchanged):
    result = func()
    while result == unchanged and datetime.now() < until:
        sleep(0.01)
        result = func()
    return result


def test_records_ok_if_site_available(checker, storage):
    checker.start_monitoring('http://httpstat.us/200')
    new_list = _wait_until_changes(storage.list, _ms_from_now(2000), [])
    assert len(new_list) == 1
    assert new_list[0].url == 'http://httpstat.us/200'
    assert new_list[0].http_code == 200
    assert new_list[0].details == ''


def test_records_connection_failure(checker, storage):
    checker.start_monitoring('http://localhost:7777')
    new_list = _wait_until_changes(storage.list, _ms_from_now(2000), [])
    assert len(new_list) == 1
    assert new_list[0].url == 'http://localhost:7777'
    assert new_list[0].http_code is None
    assert new_list[0].details == 'connection error'


def test_records_timeout_error(checker, storage):
    checker.start_monitoring('http://httpstat.us/200?sleep=5000')
    new_list = _wait_until_changes(storage.list, _ms_from_now(2000), [])
    assert len(new_list) == 1
    assert new_list[0].http_code is None
    assert new_list[0].details == 'timed out'
    assert abs(new_list[0].response_time.total_seconds() - 1.0) < 0.2


def test_records_bad_http_status(checker, storage):
    checker.start_monitoring('http://httpstat.us/500')
    new_list = _wait_until_changes(storage.list, _ms_from_now(2000), [])
    assert len(new_list) == 1
    assert new_list[0].http_code == 500


def test_checks_body_for_regex_if_specified(checker, storage):
    checker.start_monitoring('http://httpstat.us/418', page_regex='teapot')
    new_list = _wait_until_changes(storage.list, _ms_from_now(2000), [])
    assert len(new_list) == 1
    assert new_list[0].http_code == 418
    assert new_list[0].details == 'regex check passed'


def test_records_regex_check_failure(checker, storage):
    checker.start_monitoring('http://httpstat.us/418', page_regex='saucer')
    new_list = _wait_until_changes(storage.list, _ms_from_now(2000), [])
    assert len(new_list) == 1
    assert new_list[0].http_code == 418
    assert new_list[0].details == 'regex check failed'
