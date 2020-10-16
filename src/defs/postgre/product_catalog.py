"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'
BQ_EXTERNAL_CONN_ID = "fleek-prod.us.cloudsql_ktest"

PRODUCT_INFO_TABLE = "product_info"
TOP_PRODUCT_INFO_TABLE = "top_product_info"
SIMILAR_PRODUCTS_TABLE = "similar_products"

def get_full_name(table_name, staging=False):
    if staging:
        table_name = "staging_" + table_name
    return f"{PROJECT}.{table_name}"

def get_columns(table_name):
    schema = SCHEMAS.get(table_name)['schema']
    return [ c['name'] for c in schema ]

SCHEMAS = {
    PRODUCT_INFO_TABLE : {
        "schema" : [
            {
                "name": "product_id",
                "type": "bigint",
                "mode": "PRIMARY KEY"
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
                "name": "product_labels",
                "type": "TEXT []",
                "mode": "NOT NULL"
            },
            {
                "name": "product_tags",
                "type": "TEXT []",
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
                "type": "TEXT []",
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
        "tail" : f";\nCREATE INDEX IF NOT EXISTS {PROJECT}_product_info_index ON {get_full_name(PRODUCT_INFO_TABLE)} (is_active, product_id)"
    },

    SIMILAR_PRODUCTS_TABLE : {
        "schema" : [
            {
                "name": "product_id",
                "type": "bigint",
                "mode": "PRIMARY KEY"
            },
                        {
                "name": "similar_product_ids",
                "type": "BIGINT []",
                "mode": "NOT NULL"
            },
        ],
        "tail" : f";\nCREATE INDEX IF NOT EXISTS {PROJECT}_similar_items_index ON {get_full_name(SIMILAR_PRODUCTS_TABLE)} (product_id)"
    },


}

## Similar Schemas
SCHEMAS[TOP_PRODUCT_INFO_TABLE] = SCHEMAS[PRODUCT_INFO_TABLE]