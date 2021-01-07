"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'
BQ_EXTERNAL_CONN_ID = "fleek-prod.us.cloudsql_ktest"

USER_EVENTS_TABLE = "user_events"

def get_full_name(table_name, staging=False):
    if staging:
        table_name = "staging_" + table_name
    return f"{PROJECT}.{table_name}"

def get_columns(table_name):
    schema = SCHEMAS.get(table_name)['schema']
    return [ c['name'] for c in schema ]


SCHEMAS = {
    USER_EVENTS_TABLE: {
        "schema" : [
            {
                "name": "user_id",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "event",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "method",
                "type": "TEXT",
                "mode": ""
            },
            {
                "name": "event_timestamp",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "product_id",
                "type": "bigint",
                "mode": ""
            },
            {
                "name": "tags",
                "type": "TEXT[]",
                "mode": ""
            },
            {
                "name": "advertiser_names",
                "type": "TEXT[]",
                "mode": ""
            },
            {
                "name": "product_labels",
                "type": "TEXT[]",
                "mode": ""
            },
            {
                "name": "searchString",
                "type": "TEXT",
                "mode": ""
            },
        ],
        "tail" : f";\nCREATE INDEX IF NOT EXISTS {PROJECT}_user_events_table_index ON {get_full_name(USER_EVENTS_TABLE)} (event_timestamp)"
    },
}
