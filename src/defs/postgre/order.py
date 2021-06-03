"""
File containing schema
definitions for bigquery
tables.
"""
import os

from src.defs.postgre.utils import *
from src.defs.postgre import product_catalog as pcdefs

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'

ADVERTISER_TABLE_NAME = "advertiser"
ORDER_TABLE_NAME = "order"

ADVERTISER_TABLE = PostgreTable(
    name=ADVERTISER_TABLE_NAME,
    columns=[
        Column(
            name="advertiser_id",
            type="text",
            nullable=False
        ),
        Column(
            name="advertiser_name",
            type="text",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["advertiser_id"],
    ),
)

ORDER_TABLE = PostgreTable(
    name=ORDER_TABLE_NAME,
    columns=[
        Column(
            name="order_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="advertiser_name",
            type="text",
            nullable=False
        ),
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="user_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="event_timestamp",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["order_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["product_id"],
            ref_table=pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        )
    ],
    indexes=[
        Index(
            columns=["advertiser_name"],
        ),
    ]
)

TABLES.extend([
    ADVERTISER_TABLE,
    ORDER_TABLE,
])
