#!/usr/bin/env python
"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


# first: create a `turnstile` table from the turnstile topic (with 'avro' datatype!)

# second: create `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       cast the COUNT of station id to `count`
#       use value format JSON

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INT
) WITH (
    KEY='station_id',
    KAFKA_TOPIC = 'org.chicago.cta.turnstile',
    VALUE_FORMAT = 'AVRO'
);

CREATE TABLE turnstile_summary
WITH (value_format = 'JSON') AS
    Select  station_id, SUM(num_entries) as count from turnstile GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
