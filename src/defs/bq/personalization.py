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

SCHEMAS = {

    ACTIVE_PRODUCTS_TABLE : [
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
            "name": "product_id",
            "type": "INTEGER",
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
            "name": "product_price",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "product_sale_price",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "product_image_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_additional_image_url",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
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

    DAILY_CJ_DOWNLOAD_TABLE : [
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
            "name": "product_id",
            "type": "INTEGER",
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
            "name": "product_price",
            "type": "FLOAT64",
            "mode": "REQUIRED"
        },
        {
            "name": "product_sale_price",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "product_image_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_additional_image_url",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
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
