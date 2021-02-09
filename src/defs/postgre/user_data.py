"""
File containing schema
definitions for bigquery
tables.
"""
import os
from src.defs.postgre.utils import *

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'
BQ_EXTERNAL_CONN_ID = "fleek-prod.us.cloudsql_ktest"

USER_EVENTS_TABLE_NAME = "user_events"

USER_EVENTS_TABLE = PostgreTable(
    name=USER_EVENTS_TABLE_NAME,
    columns=[
        Column(
            "user_id",
            "bigint",
            nullable=False
        ),
        Column(
            "event",
            "text",
            nullable=False
        ),
        Column(
            "method",
            "text",
            nullable=True
        ),
        Column(
            "event_timestamp",
            "bigint",
            nullable=False
        ),
        Column(
            "tags",
            "TEXT[]",
            nullable=True
        ),
        Column(
            "advertiser_names",
            "text[]",
            nullable=True
        ),
        Column(
            "product_labels",
            "text[]",
            nullable=True
        ),
        Column(
            "searchString",
            "TEXT",
            nullable=True
        ),
    ]
)

TABLES.extend([
    USER_EVENTS_TABLE
])
