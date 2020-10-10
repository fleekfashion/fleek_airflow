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
from src.defs.delta.utils import GENERAL_CLUSTER_ID, SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="update_active_products_head", dag=dag)
    tail = DummyOperator(task_id="update_active_products_tail", dag=dag)
    active_to_historic_products = spark_sql_operator(
        task_id="active_to_historic_products",
        dag=dag,
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            "columns": ", ".join(pcdefs.get_columns(pcdefs.ACTIVE_PRODUCTS_TABLE)),
            "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE)
            },
        sql="template/spark_active_to_historic.sql",
        min_workers=1,
        max_workers=2
    )

    del_inactive_products = spark_sql_operator(
        task_id="delete_inactive_products",
        dag=dag,
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            },
        sql="template/spark_delete_inactive_products.sql",
        min_workers=1,
        max_workers=2
    )

    update_active_product_info = spark_sql_operator(
        task_id="update_active_product_info",
        dag=dag,
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            "columns": pcdefs.get_columns(pcdefs.PRODUCT_INFO_TABLE),
            },
        sql="template/spark_update_active_product_info.sql",
        min_workers=1,
        max_workers=2
    )

    head >> active_to_historic_products >> del_inactive_products >> tail
    head >> update_active_product_info >> tail

    return {"head": head, "tail": tail}
