import asyncio
import aiohttp
import json
import logging
import msgpack
import re
import time

from kafka.errors import KafkaError
from aiohttp import ClientSSLError, ClientConnectionError, ClientResponseError, ClientPayloadError, ClientError
from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from asyncio import wait_for, sleep, TimeoutError
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime
from json import JSONDecodeError
from marshmallow.exceptions import ValidationError
from website_checker.check_result import CheckResult, Website
from website_checker.utils import setup_termination, setup_logging, quit_if_cancelled


@dataclass
class Config:
    kafka_uri: str
    kafka_topic: str
    kafka_ca_cert_path: str
    kafka_cert_path: str
    kafka_cert_key_path: str
    check_interval: float
    websites_file: str
    user_agent: str
    check_timeout: float
    connect_timeout: float
    read_timeout: float


def config_from_env():
    from os import environ as env

    def _read_var(var_name):
        return env[var_name]

    def _read_optional_var(var_name, default):
        return env[var_name] if var_name in env else default

    return Config(
        kafka_uri=_read_var('KAFKA_URI'),
        kafka_topic=_read_var('KAFKA_TOPIC'),
        kafka_ca_cert_path=_read_var('KAFKA_CA_CERT_PATH'),
        kafka_cert_path=_read_var('KAFKA_CERT_PATH'),
        kafka_cert_key_path=_read_var('KAFKA_CERT_KEY_PATH'),
        check_interval=float(_read_var('CHECK_INTERVAL')),
        websites_file=_read_var('WEBSITES_FILE'),
        user_agent=_read_var('USER_AGENT'),
        check_timeout=float(_read_var('CHECK_TIMEOUT')),
        connect_timeout=float(_read_optional_var('CONNECT_TIMEOUT', 0.0)),
        read_timeout=float(_read_optional_var('READ_TIMEOUT', 0.0)),
    )


def validate(config):
    if config.check_timeout > config.check_interval:
        raise ValueError('check timeout cannot exceed check interval')
    if config.connect_timeout + config.read_timeout > config.check_timeout:
        raise ValueError('connect timeout + read timeout must be less than check timeout')


def create_kafka(config):
    ssl_context = create_ssl_context(
        cafile=config.kafka_ca_cert_path,
        certfile=config.kafka_cert_path,
        keyfile=config.kafka_cert_key_path,
    )
    return AIOKafkaProducer(
        bootstrap_servers=config.kafka_uri,
        security_protocol="SSL",
        ssl_context=ssl_context,
    )


def encode(check):
    return msgpack.packb(asdict(check))


async def produce(checks, kafka_client, topic):
    batch = kafka_client.create_batch()
    for check in checks:
        value = encode(check)
        batch.append(value=value, timestamp=None, key=None)
    batch.close()
    await kafka_client.send_batch(batch, topic, partition=None)


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


async def check_body(response, regex):
    if regex is None:
        return ''
    async for line_bytes in response.content:
        line = line_bytes.decode(encoding=response.charset)
        if regex.search(line) is not None:
            return 'regex check passed'
    return 'regex check failed'


async def check(website, http_session):
    logging.info('checking website %s', website.uri)
    check_result = CheckResult(website.uri, None, None, time.time())
    async with record_errors_timings(check_result):
        async with http_session.get(website.uri) as resp:
            check_result.httpcode = resp.status
            check_result.details = await check_body(resp, website.page_regex)
    return check_result


async def check_in_parallel(websites, http_session):
    return await asyncio.gather(*(check(w, http_session) for w in websites))


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


@handle_regex_compile_error()
@handle_website_schema_errors()
@handle_json_file_errors()
def read_websites(file_path):
    websites_dict = json.load(open(file_path))
    websites = Website.schema().load(websites_dict, many=True)
    for website in websites:
        if website.page_regex is not None:
            website.page_regex = re.compile(website.page_regex)
    return websites


async def periodically_check_websites(config, http_session):
    kafka_client = create_kafka(config)
    async with kafka_client:
        while True:
            websites = read_websites(config.websites_file)
            if websites is not None:
                logging.info('checking %s websites', len(websites))
                checks = await check_in_parallel(websites, http_session)
                await produce(checks, kafka_client, config.kafka_topic)
            else:
                logging.info('no websites to check')
            await sleep(config.check_interval)


@asynccontextmanager
async def retry_on_kafka_error():
    try:
        yield
    except KafkaError as e:
        logging.error('Kafka client failed, error %s', e)


def create_http_session(config):
    timeout = aiohttp.ClientTimeout(
        total=config.check_timeout,
        sock_connect=config.connect_timeout,
        sock_read=config.read_timeout,
    )
    return aiohttp.ClientSession(headers={'User-Agent': config.user_agent}, timeout=timeout)


async def run_producer():
    setup_logging()
    setup_termination()
    config = config_from_env()
    validate(config)
    async with create_http_session(config) as http_session, quit_if_cancelled():
        while True:
            async with retry_on_kafka_error():
                await periodically_check_websites(config, http_session)


def main():
    asyncio.run(run_producer())


if __name__ == "__main__":
    main()
