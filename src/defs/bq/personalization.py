"""
File containing schema
definitions for bigquery
tables.
"""
import os
from src.defs.bq import datasets

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

ACTIVE_PRODUCTS_TABLE = "active_products"
DAILY_CJ_DOWNLOAD_TABLE = "daily_cj_download"
DAILY_NEW_PRODUCT_INFO_TABLE = "daily_new_product_info"
HISTORIC_PRODUCTS_TABLE = "historic_products"


FULL_ACTIVE_PRODUCTS_TABLE = ".".join([PROJECT, datasets.PERSONALIZATION, ACTIVE_PRODUCTS_TABLE])
FULL_CJ_DOWNLOAD_TABLE = ".".join([PROJECT, datasets.PERSONALIZATION, DAILY_CJ_DOWNLOAD_TABLE])
FULL_DAILY_NEW_PRODUCT_INFO_TABLE = ".".join([
    PROJECT,
    datasets.PERSONALIZATION,
    DAILY_NEW_PRODUCT_INFO_TABLE
    ])
FULL_HISTORIC_PRODUCTS_TABLE = ".".join([PROJECT, datasets.PERSONALIZATION, HISTORIC_PRODUCTS_TABLE])

TABLE_PARTITIONS = {
        HISTORIC_PRODUCTS_TABLE : {
            "type" : "DAY",
            "field" : "execution_date"
            }
        }
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
            "name": "embeddings_production",
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
            "name": "embeddings_production",
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

}


FULL_NAMES = {}
for table_name in SCHEMAS.keys():
    FULL_NAMES[table_name] = ".".join(
        [
            PROJECT,
            datasets.PERSONALIZATION,
            ACTIVE_PRODUCTS_TABLE
        ]
    )
