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
BQ_EXTERNAL_CONN_ID = "fleek-prod.us.cloudsql_ktest"

USER_EVENTS_TABLE_NAME = "user_events"
USER_PRODUCT_FAVES_TABLE_NAME = "user_product_faves"
USER_PRODUCT_BAGS_TABLE_NAME = "user_product_bags"
USER_PRODUCT_SEENS_TABLE_NAME = "user_product_seens"

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
        Column(
            "json_data",
            "TEXT",
            nullable=True
        ),
    ]
)

USER_PRODUCT_FAVES_TABLE = PostgreTable(
    name=USER_PRODUCT_FAVES_TABLE_NAME,
    columns=[
        Column(
            "user_id",
            "bigint",
            nullable=False
        ),
        Column(
            "product_id",
            "bigint",
            nullable=False,
        ),
        Column(
            "event_timestamp",
            "bigint",
            nullable=False
        )
    ],
    primary_key=PrimaryKey(
        ["user_id", "product_id"],
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
            ["user_id", "event_timestamp"],
        )
    ]
)

USER_PRODUCT_BAGS_TABLE = PostgreTable(
    name=USER_PRODUCT_BAGS_TABLE_NAME,
    columns=[
        Column(
            "user_id",
            "bigint",
            nullable=False
        ),
        Column(
            "product_id",
            "bigint",
            nullable=False,
        ),
        Column(
            "event_timestamp",
            "bigint",
            nullable=False
        )
    ],
    primary_key=PrimaryKey(
        ["user_id", "product_id"],
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
            ["user_id", "event_timestamp"],
        )
    ]
)

USER_PRODUCT_SEENS_TABLE = PostgreTable(
    name=USER_PRODUCT_SEENS_TABLE_NAME,
    columns=[
        Column(
            "user_id",
            "bigint",
            nullable=False
        ),
        Column(
            "product_id",
            "bigint",
            nullable=False,
        ),
        Column(
            "event_timestamp",
            "bigint",
            nullable=False
        )
    ],
    primary_key=PrimaryKey(
        ["user_id", "product_id"],
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
            ["user_id", "event_timestamp"],
        )
    ]
)

TABLES.extend([
    USER_EVENTS_TABLE,
    USER_PRODUCT_FAVES_TABLE,
    USER_PRODUCT_BAGS_TABLE,
    USER_PRODUCT_SEENS_TABLE
])
