"""
File containing schema
definitions for bigquery
tables.
"""
import os

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "fleek-staging")
DATASET = "gcs_imports"

DAILY_NEW_PRODUCT_EMBEDDINGS_TABLE = "daily_new_product_embeddings"
USER_PRODUCT_RECOMMENDATIONS_TABLE = "user_product_recommendations"
TABLE_PARTITIONS = {
    }

SCHEMAS = {
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
