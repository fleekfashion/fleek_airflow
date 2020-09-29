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
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR

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
    
    head >> similar_items >> tail

    return {"head": head, "tail": tail}
