import os
from pyspark.sql.types import *
from src.defs.utils import DeltaTableDef, load_delta_schemas

DATASET = f"personalization"
USER_PRODUCT_RECS_TABLE = DeltaTableDef("user_product_recommendations", DATASET)

TABLES = {
}

load_delta_schemas(TABLES)
