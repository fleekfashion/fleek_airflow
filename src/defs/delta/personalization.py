import os
from pyspark.sql.types import *
from src.defs.utils import DeltaTableDef

PROJECT = os.environ.get("PROJECT", "staging")
PROJECT = PROJECT if PROJECT == "prod" else "staging"
DATASET = f"personalization"

USER_PRODUCT_RECS_TABLE_NAME = "user_product_recommendations"

def get_full_name(table_name):
    name = ".".join(
        [
            PROJECT + "_" + DATASET,
            table_name
        ]
    )
    return name

def get_columns(table_name):
    return TABLES[table_name]["schema"].fieldNames()

TABLES = {
}


USER_PRODUCT_RECS_TABLE = DeltaTableDef.from_tables(dataset=DATASET, tables=TABLES,
        name=USER_PRODUCT_RECS_TABLE_NAME)
