Website checker
===============

This will be a rather simple website availability checker, which shall monitor a preset list of URLs for availability.
The plan is to develop 2 backends: **checker-producer** will do the monitoring and publish results
to a Kafka topic, **checker-consumer** will read monitoring events from the same topic and store
them in a PostgreSQL database.

To run tests, execute ``make test``

Source code for the PostgreSQL schema can be found in **storage** folder.
To bootstrap a new instance of website checker database, run
``./storage/bootstrap.sh``
