Website checker
===============

This is a rather simple website availability checker, which monitors a preset list of URLs for availability.
It consists of 2 backends: **checker-producer** does the monitoring and publishes results
to a Kafka topic, **checker-consumer** reads monitoring events from the same topic and stores
them in a PostgreSQL database.

Backends accept configuration through environment variables, described in more detail below.
Both log to stderr and can be stopped with SIGTERM/SIGINT.

Tests require live Kafka and Postgres, configured in tests/setup.env.
To run tests, fill in the placeholders in tests/setup.env, then execute
``source tests/setup.env && make test``.

Producer
--------

Every CHECK_INTERVAL seconds reads contents of WEBSITES_FILE, tries to GET uri for each website,
then publishes results to Kafka as messages. Internally checks are dataclasses, encoded via msgpack
before producing them as messages.
Websites file is a text file with JSON array of objects inside.

.. code-block:: JSON
[
    {
        "uri": "the uri to check",
        "page_regex": "optional regex to search for in body"
    }
]

The regex in page_regex, if specified, is searched for on each line of response body.
If the websites file is invalid, producer will retry to read it on next interval.

Consumer
--------

Consumer connects to Kafka and consumes messages from KAFKA_TOPIC.
If you specify KAFKA_GROUP_ID, consumer will attempt to join this group.
After reading and decoding POSTGRES_BATCH_SIZE messages, consumer attempts
to insert them in one request into postgres table (see storage/schema.sql for details).
If KAFKA_GROUP_ID was specified, consumer will attempt to commit messages to Kafka
after a successful insert. Otherwise commit happens automatically.
If message could not have been decoded, it is skipped.

Storage
-------

Source code for the PostgreSQL schema can be found in **storage** folder.
To bootstrap a new instance of website checker database, run `./storage/bootstrap.sh` (requires psql).

