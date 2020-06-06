"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "fleek-staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'

PRODUCT_INFO_TABLE = "product_info"
USER_EVENTS_TABLE = "user_events"
USER_RECOMMENDATIONS_TABLE = "user_product_recommendations"

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
                "mode": "NOT NULL UNIQUE"
            },
            {
                "name": "product_additional_image_url",
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
        ],
        "tail": ""
    },
}
            
