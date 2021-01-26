import os
from pyspark.sql.types import *

PROJECT = os.environ.get("PROJECT", "staging")
PROJECT = PROJECT if PROJECT == "prod" else "staging"
DATASET = f"postgres"


def get_full_name(table_name, staging=False):
    if staging:
        table_name = "staging_" + table_name

    name = ".".join(
        [
            PROJECT + "_" + DATASET,
            table_name
        ]
    )
    return name
