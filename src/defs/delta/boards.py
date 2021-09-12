import os
from pyspark.sql.types import *

from src.defs.utils import DeltaTableDef, load_delta_schemas

DATASET = f"boards"

PRODUCT_SMART_TAG_TABLE = DeltaTableDef("product_smart_tag", DATASET)
SMART_TAG_TABLE = DeltaTableDef("smart_tag", DATASET)
ADVERTISER_SMART_TAGS = DeltaTableDef("advertiser_smart_tag", DATASET)

TABLES = {
}

load_delta_schemas(TABLES)
