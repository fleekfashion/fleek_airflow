"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "fleek-staging")
DATASET = "user_data"

AGGREGATED_USER_EVENTS_TABLE = "aggregated_user_events"
USER_EVENTS_TABLE = "user_events"

TABLE_PARTITIONS = {
    USER_EVENTS_TABLE: {
        "type" : "DAY",
        "field" : "execution_date"
    }
}

def get_full_name(table_name):
    name = ".".join(
        [
            PROJECT,
            DATASET,
            table_name
        ]
    )
    return name

def get_columns(table_name):
    schema = SCHEMAS.get(table_name)
    return [c['name'] for c in schema]

SCHEMAS = {
    AGGREGATED_USER_EVENTS_TABLE: [
        {
            "name": "user_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "product_ids",
            "type": "INTEGER",
            "mode": "REPEATED"
        },
        {
            "name": "events",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "methods",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "event_timestamps",
            "type": "INTEGER",
            "mode": "REPEATED"
        },
    ],

    USER_EVENTS_TABLE: [
        {
            "name": "user_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "event",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "event_timestamp",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "method",
            "type": "STRING",
            "mode": ""
        },
        {
            "name": "product_id",
            "type": "INTEGER",
            "mode": ""
        },
        {
            "name": "tags",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "execution_date",
            "type": "DATE",
            "mode": "REQUIRED"
        },
    ]
}
