import logging
import time
import re
import psycopg2

from aiohttp import (
    ClientSSLError,
    ClientConnectionError,
    ClientResponseError,
    ClientPayloadError,
    ClientError,
)
from asyncio import TimeoutError
from kafka.errors import KafkaError
from msgpack import FormatError, ExtraData
from contextlib import contextmanager, asynccontextmanager
from json import JSONDecodeError
from marshmallow.exceptions import ValidationError


@contextmanager
def handle_json_file_errors():
    try:
        yield
    except OSError as e:
        logging.error('failed to open websites file: %s', e)
    except JSONDecodeError as e:
        logging.error('invalid websites json, error: %s', e)


@contextmanager
def handle_website_schema_errors():
    try:
        yield
    except KeyError as e:
        logging.error('invalid websites dict, missing key: %s', e)
    except ValidationError as e:
        logging.error('invalid websites dict: %s', e)


@contextmanager
def handle_regex_compile_error():
    try:
        yield
    except re.error as e:
        logging.error('invalid regex: %s', e)


@contextmanager
def handle_unpacking_errors():
    try:
        yield
    except ExtraData:
        logging.debug('message has extra data)')
    except (ValueError, FormatError) as e:
        logging.warn('message ignored: %s', e)
    except TypeError as e:
        logging.warn('message is not a valid check result: %s', e)


@asynccontextmanager
async def handle_kafka_error():
    try:
        yield
    except KafkaError as e:
        logging.error('Kafka client failed, error %s', e)


@asynccontextmanager
async def handle_pg_error():
    try:
        yield
    except psycopg2.Warning as e:
        logging.warn('postgresql warning %s', e.msg)
    except psycopg2.Error as e:
        logging.error('postgresql error %s %s: %s', e.msg, e.pgcode, e.pgerror)


@asynccontextmanager
async def record_errors_timings(check_result):
    try:
        yield
    except TimeoutError:
        check_result.details = 'timed out'
    except ClientSSLError:
        check_result.details = 'ssl error'
    except ClientConnectionError:
        check_result.details = 'connection error'
    except ClientResponseError:
        check_result.details = 'invalid server response'
    except ClientPayloadError:
        check_result.details = 'invalid response body'
    except ClientError as e:
        check_result.details = 'client error'
        logging.error('client error: %s', e)
    check_result.response_time = time.time() - check_result.timestamp
