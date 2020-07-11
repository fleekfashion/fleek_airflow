"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "fleek-staging")
DATASET = "personalization"

ACTIVE_PRODUCTS_TABLE = "active_products"
DAILY_CJ_DOWNLOAD_TABLE = "daily_cj_download"
DAILY_NEW_PRODUCT_INFO_TABLE = "daily_new_product_info"
HISTORIC_PRODUCTS_TABLE = "historic_products"
USER_PRODUCT_RECOMMENDER_DATA = "user_product_recommender_data"

TABLE_PARTITIONS = {
        HISTORIC_PRODUCTS_TABLE : {
            "type" : "DAY",
            "field" : "execution_date"
            }
        }

def get_full_name(table_name):
    name = ".".join(
        [
            PROJECT,
            DATASET,
            table_name
        ]
    )
    return name

def get_columns(table_name):
    schema = SCHEMAS.get(table_name)
    return [c['name'] for c in schema]

SCHEMAS = {

    ACTIVE_PRODUCTS_TABLE : [
        {
            "name": "product_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_description",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_tag",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_price",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "product_brand",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_sale_price",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "product_currency",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "product_image_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_last_update",
            "type": "DATE",
            "mode": "REQUIRED"
        },
        {
            "name": "execution_date",
            "type": "DATE",
            "mode": "REQUIRED"
        },
        {
            "name": "execution_timestamp",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "product_additional_image_urls",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "advertiser_country",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "product_embedding",
            "type": "FLOAT64",
            "mode": "REPEATED"
        },
        {
            "name": "n_views",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "n_likes",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "n_add_to_cart",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "n_conversions",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
    ],

    USER_PRODUCT_RECOMMENDER_DATA: [
        {
            "name": "user_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "product_ids",
            "type": "INTEGER",
            "mode": "REPEATED"
        },
        {
            "name": "weights",
            "type": "FLOAT",
            "mode": "REPEATED"
        },
    ],

    DAILY_CJ_DOWNLOAD_TABLE : [
        {
            "name": "product_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_description",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_tag",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_price",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "product_brand",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_sale_price",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "product_currency",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "product_image_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_last_update",
            "type": "DATE",
            "mode": "REQUIRED"
        },
        {
            "name": "execution_date",
            "type": "DATE",
            "mode": "REQUIRED"
        },
        {
            "name": "execution_timestamp",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "product_additional_image_urls",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "advertiser_country",
            "type": "STRING",
            "mode": "NULLABLE"
        },
    ],
}

## Duplicate Schemas
SCHEMAS[HISTORIC_PRODUCTS_TABLE] = SCHEMAS[ACTIVE_PRODUCTS_TABLE]
SCHEMAS[DAILY_NEW_PRODUCT_INFO_TABLE] = SCHEMAS[DAILY_CJ_DOWNLOAD_TABLE]

FULL_NAMES = {}
for table_name in SCHEMAS:
    FULL_NAMES[table_name] = ".".join(
        [
            PROJECT,
            DATASET,
            table_name
        ]
    )
