"""
File containing schema
definitions for postgre
tables.
"""
import os

import sqlalchemy as sa

from src.defs.postgre import product_catalog as pcdefs
from src.defs.utils import PostgreTable, POSTGRES_METADATA, build_staging_table

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'

USER_PRODUCT_RECS_TABLE_NAME = "user_product_recommendations"

USER_PRODUCT_RECS_TABLE = PostgreTable(
    USER_PRODUCT_RECS_TABLE_NAME,
    POSTGRES_METADATA,
    sa.Column(
        "user_id",
        sa.BIGINT,
        nullable=False
        ),
    sa.Column(
        "index",
        sa.BIGINT,
        nullable=False
        ),
    sa.Column(
        "product_id",
        sa.BIGINT,
        nullable=False,
        foreign_key=sa.ForeignKey(f"{pcdefs.PRODUCT_INFO_TABLE}.product_id")
        ),
    sa.PrimaryKeyConstraint("user_id", "index",),
    sa.Index(f"{USER_PRODUCT_RECS_TABLE_NAME}_user_id_index_ix", "user_id", "index"),
    extend_existing=True
)
USER_PRODUCT_RECS_TABLE_STAGING = build_staging_table(USER_PRODUCT_RECS_TABLE)
