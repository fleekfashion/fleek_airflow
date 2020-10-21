"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, spark_sql_operator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import personalization as pdefs
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR
from src.defs.delta import user_data as user_delta

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="active_products_ml_head", dag=dag)
    tail = DummyOperator(task_id="active_products_ml_tail", dag=dag)

    similar_items = SparkScriptOperator(
        task_id="similar_items",
        dag=dag,
        script="similar_items.py",
        json_args={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE),
            "output_table": pcdefs.get_full_name(pcdefs.SIMILAR_PRODUCTS_TABLE),
            "TOP_N": 100
        },
        num_workers=5,
    )

    product_recs = SparkScriptOperator(
        task_id="product_recommendations",
        dag=dag,
        script="product_recs.py",
        json_args={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE),
            "events_table": user_delta.get_full_name(user_delta.USER_EVENTS_TABLE),
            "output_table": pdefs.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE),
            "TOP_N": 500
        },
        num_workers=4,
    )

    
    head >> similar_items >> tail
    head >> product_recs >> tail

    return {"head": head, "tail": tail}
