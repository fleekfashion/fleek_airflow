"""
File containing schema
definitions for bigquery
tables.
"""


DAILY_ACTIVE_PRODUCTS_TABLE = "daily_active_products"
DAILY_CJ_DOWNLOAD_TABLE = "daily_cj_download"
HISTORIC_PRODUCTS_TABLE = "historic_products_table"

SCHEMAS = {
    DAILY_ACTIVE_PRODUCTS_TABLE : [
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
