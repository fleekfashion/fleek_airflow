"""
File containing schema
definitions for bigquery
tables.
"""

SAGEMAKER_EMBEDDER_PRODUCT_INFO = "sagemaker_embedder_product_info"

SCHEMAS = {
    SAGEMAKER_EMBEDDER_PRODUCT_INFO : [
        {
            "name": "product_id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        {
            "name": "image_url",
            "type": "STRING",
            "mode": "REQUIRED"
        },
    ],
}
