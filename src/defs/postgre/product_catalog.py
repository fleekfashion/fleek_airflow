"""
File containing schema
definitions for bigquery
tables.
"""
import os

from src.defs.postgre import product_catalog as pcdefs
from src.defs.postgre.utils import *

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'

PRODUCT_INFO_TABLE_NAME = "product_info"
PRODUCT_PRICE_HISTORY_TABLE_NAME = "product_price_history"
SIMILAR_PRODUCTS_TABLE_NAME = "similar_products_v2"
TOP_PRODUCTS_TABLE_NAME = "top_products"

PRODUCT_INFO_TABLE = PostgreTable(
    name=PRODUCT_INFO_TABLE_NAME,
    columns=[
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="product_name",
            type="text",
            nullable=False
        ),
        Column(
            name="product_description",
            type="text",
            nullable=False
        ),
        Column(
            name="product_labels",
            type="text[]",
            nullable=False
        ),
        Column(
            name="product_tags",
            type="text[]",
            nullable=False
        ),
        Column(
            name="product_price",
            type="double precision",
            nullable=False
        ),
        Column(
            name="product_sale_price",
            type="double precision",
            nullable=False
        ),
        Column(
            name="product_image_url",
            type="text",
            nullable=False
        ),
        Column(
            name="product_additional_image_urls",
            type="text[]",
            nullable=True
        ),
        Column(
            name="product_purchase_url",
            type="text",
            nullable=False
        ),
        Column(
            name="product_brand",
            type="text",
            nullable=True
        ),
        Column(
            name="advertiser_name",
            type="text",
            nullable=False
        ),
        Column(
            name="n_views",
            type="INTEGER",
            nullable=False
        ),
        Column(
            name="n_likes",
            type="INTEGER",
            nullable=False
        ),
        Column(
            name="n_add_to_cart",
            type="INTEGER",
            nullable=False
        ),
        Column(
            name="n_conversions",
            type="INTEGER",
            nullable=False
        ),
        Column(
            name="is_active",
            type="boolean",
            nullable=False
        ),
        Column(
            name="execution_date",
            type="date",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["product_id"],
    ),
    indexes=[
        Index(
            columns=["is_active", "product_id"],
        ),
    ]
)

PRODUCT_PRICE_HISTORY_TABLE = PostgreTable(
    name=PRODUCT_PRICE_HISTORY_TABLE_NAME,
    columns=[
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="execution_date",
            type="date",
            nullable=False
        ),
        Column(
            name="product_price",
            type="double precision",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["product_id", "execution_date"]
    ),
    foreign_keys=[
        ForeignKey(
            columns=["product_id"],
            ref_table=PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        ),
    ]
)
            
SIMILAR_PRODUCTS_TABLE = PostgreTable(
    name=SIMILAR_PRODUCTS_TABLE_NAME,
    columns=[
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
        Column(
            name="index",
            type="bigint",
            nullable=False
        ),
        Column(
            name="similar_product_id",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["product_id", "index"]
    ),
    foreign_keys=[
        ForeignKey(
            columns=["similar_product_id"],
            ref_table=PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        )
    ]
)

TOP_PRODUCTS_TABLE = PostgreTable(
    name=TOP_PRODUCTS_TABLE_NAME,
    columns=[
        Column(
            name="product_id",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["product_id"]
    ),
    foreign_keys=[
        ForeignKey(
            columns=["product_id"],
            ref_table=PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"]
        )
    ]
)

TABLES.extend([
    PRODUCT_INFO_TABLE,
    PRODUCT_PRICE_HISTORY_TABLE,
    SIMILAR_PRODUCTS_TABLE,
    TOP_PRODUCTS_TABLE
])
