import os
from pyspark.sql.types import *
from src.defs.utils import DeltaTableDef, set_delta_schemas_from_tables

DATASET = f"personalization"
USER_PRODUCT_RECS_TABLE_NAME = "user_product_recommendations"

TABLES = {
}


USER_PRODUCT_RECS_TABLE = DeltaTableDef.from_tables(dataset=DATASET, tables=TABLES,
        name=USER_PRODUCT_RECS_TABLE_NAME)
