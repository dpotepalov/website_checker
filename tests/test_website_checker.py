from datetime import timedelta, datetime
from time import sleep
from website_checker.check_result import CheckResult


def _ms_from_now(x):
    return datetime.now() + timedelta(milliseconds=x)


def _wait_until_changes(func, until, unchanged):
    result = func()
    while result == unchanged and datetime.now() < until:
        sleep(0.01)
        result = func()
    return result


def test_records_ok_if_site_available(checker, storage):
    assert storage.list() == []
    checker.start_monitoring('httpstat.us/200')
    new_list = _wait_until_changes(storage.list, _ms_from_now(100), [])
    assert len(new_list) == 1
    assert new_list[0].website == 'httpstat.us/200'
    assert new_list[0].httpcode == 200
    # TODO: check time-related members