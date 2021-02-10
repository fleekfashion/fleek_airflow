"""
File containing schema
definitions for postgre
tables.
"""
import os

from src.defs.postgre import product_catalog as pcdefs
from src.defs.postgre import utils as u

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'

USER_PRODUCT_RECS_TABLE_NAME = "user_product_recommendations"

USER_PRODUCT_RECS_TABLE = u.PostgreTable(
    name=USER_PRODUCT_RECS_TABLE_NAME,
    columns=[
        u.Column(
            "user_id",
            "BIGINT",
            nullable=False
        ),
        u.Column(
            "index",
            "BIGINT",
            nullable=False
        ),
        u.Column(
            "product_id",
            "BIGINT",
            nullable=False,
        ),
    ],
    primary_key=u.PrimaryKey(["user_id", "index"], name=f"pk_{PROJECT}_user_product_recs"),
    indexes=[
        u.Index(
            ["user_id", "index"], 
            name=f"{PROJECT}_user_product_recs_index"
        )
    ],
    foreign_keys=[
        u.ForeignKey(
            columns=["product_id"],
            ref_table=pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
            ref_columns=["product_id"],
            name=f"{USER_PRODUCT_RECS_TABLE_NAME}_product_id_fkey",
        )
    ]
)

u.TABLES.extend([
    USER_PRODUCT_RECS_TABLE,
])
