"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "fleek-staging")
DATASET = "gcs_exports"

# Table Names
SAGEMAKER_EMBEDDER_PRODUCT_INFO = "sagemaker_embedder_product_info"
USER_RECOMMENDATIONS_TABLE = "user_recommendations"

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

def get_full_name(table_name):
    name = ".".join(
        [
            PROJECT,
            DATASET,
            table_name
        ]
    )
    return name

FULL_NAMES = {}
for table_name in SCHEMAS:
    FULL_NAMES[table_name] = ".".join(
        [
            PROJECT,
            DATASET,
            table_name
        ]
    )
