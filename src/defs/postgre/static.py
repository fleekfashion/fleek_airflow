"""
File containing schema
definitions for bigquery
tables.
"""
import os

from src.defs.postgre import product_catalog as pcdefs
from src.defs.postgre.utils import *

SYNONYMS_TABLE_NAME = 'synonym'

SYNONYMS_TABLE = PostgreTable(
    name=SYNONYMS_TABLE_NAME,
    columns=[
        Column(
            name="root",
            type="text",
            nullable=False
        ),
        Column(
            name="synonym",
            type="text",
            nullable=False
        ),
    ],
    primary_key=PrimaryKey(
        columns=["root", "synonym"]
    )
)


TABLES.extend([
    SYNONYMS_TABLE,
])
