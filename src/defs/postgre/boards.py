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

BOARD_TABLE_NAME = "board"
BOARD_TYPE_TABLE_NAME = "board_type"
BOARD_PRODUCT_TABLE_NAME = "board_product"
USER_BOARD_TABLE_NAME = "user_board"
REJECTED_BOARD_TABLE_NAME = "rejected_board"

BOARD_TABLE = PostgreTable(
    name=BOARD_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="creation_date",
            type="date",
            nullable=False
        ),
        Column(
            name="name",
            type="text",
            nullable=False
        ),
        Column(
            name="description",
            type="text",
            nullable=True
        ),
        Column(
            name="artwork_url",
            type="text",
            nullable=True
        ),
        Column(
            name="last_modified_timestamp",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id"],
    ),
)

BOARD_TYPE_TABLE = PostgreTable(
    name=BOARD_TYPE_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="is_user_generated",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_smart",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_price_drop",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_all_faves",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_global",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_daily_mix",
            type="boolean",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_TABLE.get_full_name(),
            ref_columns=["board_id"]
        )
    ],
    indexes=[
        Index(
            columns=["is_user_generated"],
        ),
        Index(
            columns=["is_smart"],
        ),
        Index(
            columns=["is_price_drop"],
        ),
        Index(
            columns=["is_all_faves"],
        ),
        Index(
            columns=["is_global"],
        ),
        Index(
            columns=["is_daily_mix"],
        ),
    ]
)

BOARD_PRODUCT_TABLE = PostgreTable(
    name=BOARD_PRODUCT_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="last_modified_timestamp",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id", "product_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_TABLE.get_full_name(),
            ref_columns=["board_id"]
        ),
        ForeignKey(
            columns=["product_id"],
            ref_table=pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        )
    ],
)

USER_BOARD_TABLE = PostgreTable(
    name=USER_BOARD_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="user_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="is_owner",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_collaborator",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_following",
            type="boolean",
            nullable=False
        ),
        Column(
            name="is_suggested",
            type="boolean",
            nullable=False
        ),
        Column(
            name="last_modified_timestamp",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["user_id", "board_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_TABLE.get_full_name(),
            ref_columns=["board_id"]
        )
    ],
    indexes=[
        Index(
            columns=["board_id", "user_id"],
        ),
        Index(
            columns=["is_collaborator"],
        ),
        Index(
            columns=["is_following"],
        ),
        Index(
            columns=["is_suggested"],
        ),
    ]
)

REJECTED_BOARD_TABLE = PostgreTable(
    name=REJECTED_BOARD_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="user_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="last_modified_timestamp",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["user_id", "board_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_TABLE.get_full_name(),
            ref_columns=["board_id"]
        ),
    ],
    indexes=[
        Index(
            columns=["board_id"],
        ),
        Index(
            columns=["user_id"],
        ),
        Index(
            columns=["last_modified_timestamp"],
        ),
    ]
)


TABLES.extend([
    BOARD_TABLE,
    BOARD_TYPE_TABLE,
    BOARD_PRODUCT_TABLE,
    USER_BOARD_TABLE,
    REJECTED_BOARD_TABLE
])
