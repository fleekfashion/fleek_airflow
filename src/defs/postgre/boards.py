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
SMART_TAG_TABLE_NAME = "smart_tag"
BOARD_SMART_TAG_TABLE_NAME = "board_smart_tag"
BOARD_PRODUCT_TABLE_NAME = "board_product"
PRODUCT_SMART_TAG_TABLE_NAME = 'product_smart_tag'
USER_BOARD_TABLE_NAME = "user_board"
REJECTED_BOARD_TABLE_NAME = "rejected_board"
REJECTED_BOARD_SMART_TAG_POPUP_TABLE_NAME = "rejected_board_smart_tag_popup"

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
        Column(
            name="board_type",
            type="text",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["board_id"],
    ),
)

SMART_TAG_TABLE = PostgreTable(
    name=SMART_TAG_TABLE_NAME,
    columns=[
        Column(
            name="smart_tag_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="product_label",
            type="text",
            nullable=True
        ),
        Column(
            name="product_secondary_labels",
            type="text[]",
            nullable=True
        ),
        Column(
            name="suggestion",
            type="text",
            nullable=False
        ),
        Column(
            name="n_hits",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["smart_tag_id"],
    ),
)

BOARD_SMART_TAG_TABLE = PostgreTable(
    name=BOARD_SMART_TAG_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name="smart_tag_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name='useless',
            type='bool',
            nullable=True
        )
    ],
    primary_key=PrimaryKey(
        columns=["board_id", "smart_tag_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_TABLE.get_full_name(),
            ref_columns=["board_id"]
        ),
        ForeignKey(
            columns=["smart_tag_id"],
            ref_table=SMART_TAG_TABLE.get_full_name(),
            ref_columns=["smart_tag_id"]
        ),
    ],
)

PRODUCT_SMART_TAG_TABLE = PostgreTable(
    name=PRODUCT_SMART_TAG_TABLE_NAME,
    columns=[
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="smart_tag_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name='useless',
            type='bool',
            nullable=True
        )

    ],
    primary_key=PrimaryKey(
        columns=["product_id", "smart_tag_id"],
    ),
    foreign_keys=[
        ForeignKey(
            columns=["product_id"],
            ref_table=pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        ),
        ForeignKey(
            columns=["smart_tag_id"],
            ref_table=SMART_TAG_TABLE.get_full_name(),
            ref_columns=["smart_tag_id"]
        )
    ],
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

REJECTED_BOARD_SMART_TAG_POPUP_TABLE = PostgreTable(
    name=REJECTED_BOARD_SMART_TAG_POPUP_TABLE_NAME,
    columns=[
        Column(
            name="board_id",
            type="uuid",
            nullable=False
        ),
        Column(
            name='useless',
            type='bool',
            nullable=True
        )
    ],
    primary_key=PrimaryKey(
        columns=["board_id"]
    ),
    foreign_keys=[
        ForeignKey(
            columns=["board_id"],
            ref_table=BOARD_TABLE.get_full_name(),
            ref_columns=["board_id"]
        ),
    ],
)

TABLES.extend([
    BOARD_TABLE,
    SMART_TAG_TABLE,
    BOARD_SMART_TAG_TABLE,
    BOARD_PRODUCT_TABLE,
    PRODUCT_SMART_TAG_TABLE,
    USER_BOARD_TABLE,
    REJECTED_BOARD_TABLE,
    REJECTED_BOARD_SMART_TAG_POPUP_TABLE
])
