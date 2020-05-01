"""
File containing schema
definitions for bigquery
tables.
"""


DAILY_CJ_DOWNLOAD_TABLE = "daily_cj_download"
DAILY_CJ_DOWNLOAD_TABLE_SCHEMA = [
    {
        "name": "execution_date_timestamp",
        "type": "TIMESTAMP",
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
]
