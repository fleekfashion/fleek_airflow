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
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_category",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "catalog_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "product_embedding",
            "type": "FLOAT64",
            "mode": "REPEATED"
        },
        {
            "name": "currency",
            "type": "STRING",
            "mode": "REQUIRED"
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
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_category",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "catalog_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "currency",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ],

    DAILY_NEW_PRODUCT_INFO_TABLE: [
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
            "name": "product_purchase_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_name",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "advertiser_category",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "catalog_id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "currency",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ],

    HISTORIC_PRODUCTS_TABLE : [
        {
            "name": "execution_date",
            "type": "date",
            "mode": "required"
        },
        {
            "name": "execution_timestamp",
            "type": "integer",
            "mode": "required"
        },
        {
            "name": "product_id",
            "type": "integer",
            "mode": "required"
        },
        {
            "name": "product_name",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "product_description",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "product_tag",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "product_price",
            "type": "float64",
            "mode": "required"
        },
        {
            "name": "product_sale_price",
            "type": "float64",
            "mode": "nullable"
        },
        {
            "name": "product_image_url",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "product_purchase_url",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "advertiser_id",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "advertiser_name",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "advertiser_category",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "catalog_id",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "product_embedding",
            "type": "float64",
            "mode": "repeated"
        },
        {
            "name": "currency",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "n_views",
            "type": "integer",
            "mode": "required"
        },
        {
            "name": "n_likes",
            "type": "integer",
            "mode": "required"
        },
        {
            "name": "n_add_to_cart",
            "type": "integer",
            "mode": "required"
        },
        {
            "name": "n_conversions",
            "type": "integer",
            "mode": "required"
        },
    ],

}


FULL_NAMES = {}
for table_name in SCHEMAS:
    FULL_NAMES[table_name] = ".".join(
        [
            PROJECT,
            DATASET,
            table_name
        ]
    )
