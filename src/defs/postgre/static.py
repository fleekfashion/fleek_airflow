"""
File containing schema
definitions for bigquery
tables.
"""
import os

from src.defs.postgre import product_catalog as pcdefs
from src.defs.postgre.utils import *

TRENDING_SEARCHES_TABLE_NAME = 'trending_searches'
LABEL_SEARCHES_TABLE_NAME = 'label_searches'

TRENDING_SEARCHES_TABLE = PostgreTable(
    name=TRENDING_SEARCHES_TABLE_NAME,
    columns=[
        Column(
            name="suggestion",
            type="text",
            nullable=False
        ),
        Column(
            name="product_label",
            type="text",
            nullable=True
        ),
        Column(
            name="product_image_url",
            type="text",
            nullable=False
        ),
        Column(
            name="rank",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["suggestion"]
    )
)

LABEL_SEARCHES_TABLE = PostgreTable(
    name=LABEL_SEARCHES_TABLE_NAME,
    columns=[
        Column(
            name="suggestion",
            type="text",
            nullable=False
        ),
        Column(
            name="product_label",
            type="text",
            nullable=True
        ),
        Column(
            name="product_image_url",
            type="text",
            nullable=False
        ),
        Column(
            name="rank",
            type="bigint",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["suggestion"]
    )
)

TABLES.extend([

])
