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

BOARD_INFO_TABLE_NAME = "board_info"
BOARD_TYPE_TABLE_NAME = "board_type"
BOARD_PRODUCTS_TABLE_NAME = "board_products"
BOARD_USERS_TABLE_NAME = "board_users"
REJECTED_BOARDS_TABLE_NAME = "rejected_boards"

BOARD_INFO_TABLE = PostgreTable(
    name=BOARD_INFO_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="bigint",
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
            nullable=False
        ),
        Column(
            name="last_modified",
            type="date",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id"],
    ),
    indexes=[
        Index(
            columns=["board_id"],
        ),
    ]
)

BOARD_TYPE_TABLE = PostgreTable(
    name=BOARD_TYPE_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="bigint",
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
            ref_table=BOARD_INFO_TABLE.get_full_name(),
            ref_columns=["board_id"]
        )
    ],
    indexes=[
        Index(
            columns=["board_id", "is_smart", "is_price_drop", "is_all_faves", "is_global", "is_daily_mix"],
        ),
    ]
)

BOARD_PRODUCTS_TABLE = PostgreTable(
    name=BOARD_PRODUCTS_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="date_added",
            type="date",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id", "product_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_INFO_TABLE.get_full_name(),
            ref_columns=["board_id"]
        ),
        ForeignKey(
            columns=["product_id"],
            ref_table=pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        )
    ],
    indexes=[
        Index(
            columns=["board_id"],
        ),
    ]
)

BOARD_USERS_TABLE = PostgreTable(
    name=BOARD_USERS_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="bigint",
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
            name="date_added",
            type="date",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_INFO_TABLE.get_full_name(),
            ref_columns=["board_id"]
        )
    ],
    indexes=[
        Index(
            columns=["board_id", "user_id", "is_owner", "is_collaborator", "is_following", "is_suggested"],
        ),
    ]
)

REJECTED_BOARDS_TABLE = PostgreTable(
    name=REJECTED_BOARDS_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="user_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="date_rejected",
            type="date",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_INFO_TABLE.get_full_name(),
            ref_columns=["board_id"]
        ),
    ],
    indexes=[
        Index(
            columns=["board_id","user_id"],
        ),
    ]
)


TABLES.extend([
    BOARD_INFO_TABLE,
    BOARD_TYPE_TABLE,
    BOARD_PRODUCTS_TABLE,
    BOARD_USERS_TABLE,
    REJECTED_BOARDS_TABLE
])
