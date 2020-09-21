"""
File containing schema
definitions for bigquery
tables.
"""
import os
from . import utils

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'
BQ_EXTERNAL_CONN_ID = "fleek-prod.us.cloudsql_ktest"

PRODUCT_INFO_TABLE = "product_info"
TOP_PRODUCT_INFO_TABLE = "top_product_info"
USER_BATCH_TABLE = "user_batch"
USER_EVENTS_TABLE = "user_events"
USER_RECOMMENDATIONS_TABLE = "user_product_recommendations"

def get_full_name(table_name):
    return table_name 

def get_columns(table_name):
    schema = SCHEMAS.get(table_name)['schema']
    return [ c['name'] for c in schema ]

SCHEMAS = {

    PRODUCT_INFO_TABLE : {
        "schema" : [
            {
                "name": "product_id",
                "type": "bigint",
                "mode": "NOT NULL UNIQUE"
            },
            {
                "name": "product_name",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "product_description",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "product_tag",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "product_price",
                "type": "double precision",
                "mode": "NOT NULL"
            },
            {
                "name": "product_sale_price",
                "type": "double precision",
                "mode": ""
            },
            {
                "name": "product_image_url",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "product_additional_image_urls",
                "type": "TEXT",
                "mode": ""
            },
            {
                "name": "product_purchase_url",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "product_brand",
                "type": "TEXT",
                "mode": ""
            },
            {
                "name": "advertiser_name",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "n_views",
                "type": "INTEGER",
                "mode": "NOT NULL"
            },
            {
                "name": "n_likes",
                "type": "INTEGER",
                "mode": "NOT NULL"
            },
            {
                "name": "n_add_to_cart",
                "type": "INTEGER",
                "mode": "NOT NULL"
            },
            {
                "name": "n_conversions",
                "type": "INTEGER",
                "mode": "NOT NULL"
            },
            {
                "name": "is_active",
                "type": "BOOLEAN",
                "mode": "NOT NULL"
            },
            {
                "name": "execution_date",
                "type": "DATE",
                "mode": "NOT NULL"
            },
        ],
        "tail" : f";\nCREATE INDEX ON {PRODUCT_INFO_TABLE} (product_id)"
    },

    USER_EVENTS_TABLE: {
        "schema" : [
            {
                "name": "user_id",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "event",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
            {
                "name": "event_timestamp",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "method",
                "type": "TEXT",
                "mode": ""
            },
            {
                "name": "product_id",
                "type": "bigint",
                "mode": ""
            },
            {
                "name": "tags",
                "type": "TEXT[]",
                "mode": ""
            },
        ],
        "tail": ""
    },

    USER_BATCH_TABLE: {
        "schema" : [
            {
                "name": "user_id",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "batch",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "last_filter",
                "type": "TEXT",
                "mode": "NOT NULL"
            },
        ],
        "tail" : f";\nCREATE INDEX ON {USER_BATCH_TABLE} (user_id)"
        }
}

## Similar Schemas
SCHEMAS[TOP_PRODUCT_INFO_TABLE] = SCHEMAS[PRODUCT_INFO_TABLE]
