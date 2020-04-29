"""
File containing schema
definitions for bigquery
tables.
"""

PERSONALIZATION = {

        ## Daily job to download
        ## CJ affiliate data.
        "daily_cj_download" : [
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
                "mode": "REQUIRED"
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
    }
