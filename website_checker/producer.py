import asyncio
import aiohttp
import json
import logging
import msgpack
import re
import time

from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from asyncio import wait_for, sleep
from dataclasses import dataclass, asdict
from datetime import datetime
from website_checker.error_handlers import (
    handle_kafka_error,
    handle_json_file_errors,
    handle_regex_compile_error,
    handle_website_schema_errors,
    record_errors_timings,
)
from website_checker.structs import CheckResult, Website
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


def _config_from_env():
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


def _validate(config):
    if config.check_timeout > config.check_interval:
        raise ValueError('check timeout cannot exceed check interval')
    if config.connect_timeout + config.read_timeout > config.check_timeout:
        raise ValueError('connect timeout + read timeout must be less than check timeout')


def _create_kafka(config):
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


def _create_http_session(config):
    timeout = aiohttp.ClientTimeout(
        total=config.check_timeout,
        sock_connect=config.connect_timeout,
        sock_read=config.read_timeout,
    )
    return aiohttp.ClientSession(headers={'User-Agent': config.user_agent}, timeout=timeout)


def _encode(check):
    return msgpack.packb(asdict(check))


async def _produce(checks, kafka_client, topic):
    batch = kafka_client.create_batch()
    for check in checks:
        value = _encode(check)
        batch.append(value=value, timestamp=None, key=None)
    batch.close()
    await kafka_client.send_batch(batch, topic, partition=None)


async def _check_body(response, regex):
    if regex is None:
        return ''
    async for line_bytes in response.content:
        line = line_bytes.decode(encoding=response.charset)
        if regex.search(line) is not None:
            return 'regex check passed'
    return 'regex check failed'


async def _check(website, http_session):
    logging.info('checking website %s', website.uri)
    check_result = CheckResult(website.uri, None, None, time.time())
    async with record_errors_timings(check_result):
        async with http_session.get(website.uri) as resp:
            check_result.httpcode = resp.status
            check_result.details = await _check_body(resp, website.page_regex)
    return check_result


async def _check_in_parallel(websites, http_session):
    return await asyncio.gather(*(_check(w, http_session) for w in websites))


@handle_regex_compile_error()
@handle_website_schema_errors()
@handle_json_file_errors()
def _read_websites(file_path):
    websites_dict = json.load(open(file_path))
    websites = Website.schema().load(websites_dict, many=True)
    for website in websites:
        if website.page_regex is not None:
            website.page_regex = re.compile(website.page_regex)
    return websites


async def _periodically_check_websites(config, http_session):
    kafka_client = _create_kafka(config)
    async with kafka_client:
        while True:
            websites = _read_websites(config.websites_file)
            if websites is not None:
                logging.info('checking %s websites', len(websites))
                checks = await _check_in_parallel(websites, http_session)
                await _produce(checks, kafka_client, config.kafka_topic)
            else:
                logging.info('no websites to check')
            await sleep(config.check_interval)


async def _run_producer():
    setup_logging()
    setup_termination()
    config = _config_from_env()
    _validate(config)
    async with _create_http_session(config) as http_session, quit_if_cancelled():
        while True:
            async with handle_kafka_error():
                await _periodically_check_websites(config, http_session)


def main():
    asyncio.run(_run_producer())


if __name__ == "__main__":
    main()
