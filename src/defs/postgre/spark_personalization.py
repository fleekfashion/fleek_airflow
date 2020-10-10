"""
File containing schema
definitions for postgre
tables.
"""
import os
from src.defs.postgre import product_catalog as pcdefs

PROJECT = os.environ.get("PROJECT", "staging")
INSTANCE = "fleek-app-prod1"
DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'
BQ_EXTERNAL_CONN_ID = "fleek-prod.us.cloudsql_ktest"

USER_PRODUCT_RECS_TABLE = "user_product_recommendations"

def get_full_name(table_name, staging=False):
    if staging:
        table_name = "staging_" + table_name
    return f"{PROJECT}.{table_name}"

def get_columns(table_name):
    schema = SCHEMAS.get(table_name)['schema']
    return [ c['name'] for c in schema ]

SCHEMAS = {
    USER_PRODUCT_RECS_TABLE: {
        "schema" : [
            {
                "name": "user_id",
                "type": "bigint",
                "mode": "NOT NULL",
            },
            {
                "name": "index",
                "type": "bigint",
                "mode": "NOT NULL"
            },
            {
                "name": "product_id",
                "type": "bigint",
                "mode": f"NOT NULL",
                "prod": (
                    f"REFERENCES {pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE)} (product_id),\n"
                    f"constraint pk_{PROJECT}_user_product_recs primary key (user_id, index)"
                )
            },
    ],
        "tail": f";\nCREATE INDEX IF NOT EXISTS {PROJECT}_user_product_recs_index ON {get_full_name(USER_PRODUCT_RECS_TABLE)} (user_id, index)",
    }
}
