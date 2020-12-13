DROP SCHEMA IF EXISTS checker;

CREATE SCHEMA checker;

CREATE TABLE checker.results(
    url text PRIMARY KEY,
    http_code int,
    details text
);

CREATE VIEW checker.list_results AS
SELECT
    url,
    http_code,
    details
FROM
    checker.results;