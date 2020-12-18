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
from datetime import timedelta, datetime, timezone
from website_checker.error_handlers import (
    handle_kafka_error,
    handle_pg_error,
    handle_unpacking_errors,
)
from website_checker.structs import CheckResult
from website_checker.utils import setup_termination, setup_logging, quit_if_cancelled


@dataclass
class Config:
    consumer_timeout: float
    storage_timeout: float
    postgres_uri: str
    postgres_batch_size: int
    kafka_uri: str
    kafka_topic: str
    kafka_group_id: str
    kafka_ca_cert_path: str
    kafka_cert_path: str
    kafka_cert_key_path: str


def _config_from_env():
    from os import environ as env

    def _read_var(var_name):
        return env[var_name]

    def _read_optional_var(var_name, default=None):
        return env[var_name] if var_name in env else default

    return Config(
        consumer_timeout=float(_read_optional_var('CONSUMER_TIMEOUT', 1.0)),
        storage_timeout=float(_read_optional_var('STORAGE_TIMEOUT', 1.0)),
        postgres_uri=_read_var('POSTGRES_URI'),
        postgres_batch_size=int(_read_optional_var('POSTGRES_BATCH_SIZE', 1.0)),
        kafka_uri=_read_var('KAFKA_URI'),
        kafka_topic=_read_var('KAFKA_TOPIC'),
        kafka_group_id=_read_optional_var('KAFKA_GROUP_ID'),
        kafka_ca_cert_path=_read_var('KAFKA_CA_CERT_PATH'),
        kafka_cert_path=_read_var('KAFKA_CERT_PATH'),
        kafka_cert_key_path=_read_var('KAFKA_CERT_KEY_PATH')
    )


class Batch:
    checks = []

    def __init__(self, max_size):
        self.max_size = max_size

    def append(self, check):
        if self.is_full():
            raise RuntimeError('batch full, cannot append!')
        self.checks.append(check)

    def is_full(self):
        return len(self.checks) >= self.max_size

    def clear(self):
        self.checks = []


async def _create_pg_pool(config):
    return await aiopg.create_pool(dsn=config.postgres_uri, minsize=0, maxsize=1)


def _create_kafka(config):
    ssl_context = create_ssl_context(
        cafile=config.kafka_ca_cert_path,
        certfile=config.kafka_cert_path,
        keyfile=config.kafka_cert_key_path,
    )
    return AIOKafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_uri,
        group_id=config.kafka_group_id,
        auto_offset_reset='latest',
        security_protocol="SSL",
        ssl_context=ssl_context,
    )


@handle_unpacking_errors()
def _decode(message):
    unpacked = msgpack.unpackb(message.value, raw=False)
    return CheckResult(**unpacked)


def _insert_sql(checks):
    values_template = ','.join(['%s'] * len(checks))
    return 'INSERT INTO checker.results VALUES {}'.format(values_template)


def _pg_tuple(check):
    return (
        check.website,
        check.httpcode,
        timedelta(seconds=check.response_time),
        datetime.fromtimestamp(check.timestamp, timezone.utc),
        check.details,
    )


async def _store(checks, pool):
    async with pool.acquire() as conn, handle_pg_error():
        async with conn.cursor() as cursor:
            await cursor.execute(_insert_sql(checks), [_pg_tuple(c) for c in checks])


async def _batch_store(check, batch, pool):
    batch.append(check)
    if batch.is_full():
        await _store(batch.checks, pool)
        batch.clear()


async def _consume_checks(config, pg_pool):
    async with _create_kafka(config) as kafka_client, handle_kafka_error():
        batch = Batch(config.postgres_batch_size)
        async for message in kafka_client:
            check = _decode(message)
            if check is not None:
                await _batch_store(check, batch, pg_pool)


async def _run_consumer():
    setup_logging()
    setup_termination()
    config = _config_from_env()
    pg_pool = await _create_pg_pool(config)
    async with pg_pool, quit_if_cancelled():
        while True:
            await _consume_checks(config, pg_pool)


def main():
    asyncio.run(_run_consumer())


if __name__ == "__main__":
    main()
