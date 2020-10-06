import os
from pyspark.sql.types import *

PROJECT = os.environ.get("PROJECT", "staging")
PROJECT = PROJECT if PROJECT == "prod" else "staging"
DATASET = f"{PROJECT}_personalization"

USER_PRODUCT_RECS_TABLE = "user_product_recommendations"

def get_full_name(table_name):
    name = ".".join(
        [
            DATASET,
            table_name
        ]
    )
    return name

def get_columns(table_name):
    return TABLES[table_name]["schema"].fieldNames()

TABLES = {
}
