import asyncio
import aiopg
import logging
import msgpack
import psycopg2
import signal
import time

from kafka.errors import KafkaError
from aiokafka import AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
from asyncio import wait_for, sleep
from contextlib import asynccontextmanager
from dataclasses import dataclass, astuple
from datetime import timedelta, datetime
from msgpack import FormatError, ExtraData
from website_checker.check_result import CheckResult
from website_checker.utils import setup_termination, setup_logging, quit_if_cancelled


@dataclass
class Config:
    consumer_timeout: float
    storage_timeout: float
    storage_dsn: str
    kafka_uri: str
    kafka_topic: str
    kafka_ca_cert_path: str
    kafka_cert_path: str
    kafka_cert_key_path: str


def config_from_env():
    from os import environ as env

    def _read_var(var_name):
        return env[var_name]

    def _read_optional_var(var_name, default=None):
        return env[var_name] if var_name in env else default

    return Config(
        consumer_timeout=float(_read_optional_var('CONSUMER_TIMEOUT', 1.0)),
        storage_timeout=float(_read_optional_var('STORAGE_TIMEOUT', 1.0)),
        storage_dsn=_read_var('STORAGE_DSN'),
        kafka_uri=_read_var('KAFKA_URI'),
        kafka_topic=_read_var('KAFKA_TOPIC'),
        kafka_ca_cert_path=_read_var('KAFKA_CA_CERT_PATH'),
        kafka_cert_path=_read_var('KAFKA_CERT_PATH'),
        kafka_cert_key_path=_read_var('KAFKA_CERT_KEY_PATH')
    )


@asynccontextmanager
async def quit_on_kafka_error():
    try:
        yield
    except KafkaError as e:
        # TODO: what if it is retriable? resetting the client
        # may be overkill in such case, though still a
        # possible course of action.
        logging.error('consume Kafka failed, error %s', e)


@asynccontextmanager
async def log_pg_error():
    try:
        yield
    except psycopg2.Warning as e:
        logging.warn('postgresql warning %s', e.msg)
    except psycopg2.Error as e:
        logging.error('postgresql error %s %s: %s', e.msg, e.pgcode, e.pgerror)


async def create_pg_pool(config):
    return await aiopg.create_pool(dsn=config.storage_dsn, minsize=0, maxsize=1)


def create_kafka(config):
    ssl_context = create_ssl_context(
        cafile=config.kafka_ca_cert_path,
        certfile=config.kafka_cert_path,
        keyfile=config.kafka_cert_key_path,
    )
    return AIOKafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_uri,
        auto_offset_reset='latest',
        security_protocol="SSL",
        ssl_context=ssl_context,
    )


def decode(message):
    try:
        unpacked = msgpack.unpackb(message.value, raw=False)
        try:
            return CheckResult(**unpacked)
        except TypeError as e:
            logging.warn('message unpacked to %s, but not a valid check result: %s', unpacked, e)
            return None
    except ExtraData:
        logging.debug('message %s at offset %d has extra data)', message.key, message.offset)
    except (ValueError, FormatError) as e:
        logging.warn('message %s at offset %d ignored: %s', message.key, message.offset, e)
        return None


def _insert_sql(checks):
    values_template = ','.join(['%s'] * len(checks))
    return 'INSERT INTO checker.results VALUES {}'.format(values_template)


def _pg_tuple(check):
    return (
        check.website,
        check.httpcode,
        timedelta(seconds=check.response_time),
        datetime.utcfromtimestamp(check.timestamp),
        check.details,
    )


async def store(checks, pool):
    async with pool.acquire() as conn, log_pg_error():
        async with conn.cursor() as cursor:
            await cursor.execute(_insert_sql(checks), [_pg_tuple(c) for c in checks])


async def consume_checks(config, pg_pool):
    async with create_kafka(config) as kafka_client, quit_on_kafka_error():
        async for message in kafka_client:
            check = decode(message)
            if check is not None:
                # TODO: add batching for checks
                await store([check], pg_pool)


async def run_consumer():
    setup_logging()
    setup_termination()
    config = config_from_env()
    pg_pool = await create_pg_pool(config)
    async with pg_pool, quit_if_cancelled():
        while True:
            await consume_checks(config, pg_pool)


def main():
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
