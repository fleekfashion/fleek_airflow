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
ORDER_PRODUCT_TABLE_NAME = "order_product"

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
            name="user_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="event_timestamp",
            type="bigint",
            nullable=False
        ),
        Column(
            name="address1",
            type="text",
            nullable=False
        ),
        Column(
            name="address2",
            type="text",
            nullable=True
        ),
        Column(
            name="city",
            type="text",
            nullable=False
        ),
        Column(
            name="company",
            type="text",
            nullable=True
        ),
        Column(
            name="country",
            type="text",
            nullable=False
        ),
        Column(
            name="first_name",
            type="text",
            nullable=True
        ),
        Column(
            name="last_name",
            type="text",
            nullable=True
        ),
        Column(
            name="phone",
            type="text",
            nullable=False
        ),
        Column(
            name="province",
            type="text",
            nullable=False
        ),
        Column(
            name="zip",
            type="text",
            nullable=False
        ),
        Column(
            name="province_code",
            type="text",
            nullable=True
        ),
        Column(
            name="country_code",
            type="text",
            nullable=True
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
        ),
    ],
    indexes=[
        Index(
            columns=["user_id"],
        ),
    ]
)

ORDER_PRODUCT_TABLE = PostgreTable(
    name=ORDER_PRODUCT_TABLE_NAME,
    columns=[
        Column(
            name="order_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="size",
            type="text",
            nullable=False
        ),
        Column(
            name="quantity",
            type="smallint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["order_id", "product_id", "size"],
    ),
)

TABLES.extend([
    ADVERTISER_TABLE,
    ORDER_TABLE,
    ORDER_PRODUCT_TABLE,
])
