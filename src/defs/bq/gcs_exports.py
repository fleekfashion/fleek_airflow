"""
File containing schema
definitions for bigquery
tables.
"""
import os
from src.defs.bq import datasets

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

# Table Names
SAGEMAKER_EMBEDDER_PRODUCT_INFO = "sagemaker_embedder_product_info"

# Full Table Names
FULL_SAGEMAKER_EMBEDDER_PRODUCT_INFO = ".".join(
        [
            PROJECT,
            datasets.GCS_EXPORTS,
            SAGEMAKER_EMBEDDER_PRODUCT_INFO
        ]
    )

# Dict of Table Partitions
TABLE_PARTITIONS = {
        }


# Schemas of each table
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
