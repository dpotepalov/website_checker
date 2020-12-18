DROP SCHEMA IF EXISTS checker CASCADE;

CREATE SCHEMA checker;

CREATE TABLE checker.results(
    url text,
    http_code int,
    response_time INTERVAL,
    check_timestamp timestamptz,
    details text,
    PRIMARY KEY(url, check_timestamp)
);

CREATE VIEW checker.list_results AS
SELECT
    url,
    http_code,
    response_time,
    check_timestamp,
    details
FROM
    checker.results;