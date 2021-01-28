"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    with TaskGroup(group_id="update_active_products", dag=dag) as group:
        active_to_historic_products = SparkSQLOperator(
            task_id="active_to_historic_products",
            dag=dag,
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "product_info_table": pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
                "columns": ", ".join(pcdefs.ACTIVE_PRODUCTS_TABLE.get_columns()),
                "historic_table": pcdefs.HISTORIC_PRODUCTS_TABLE.get_full_name()
                },
            sql="template/spark_active_to_historic.sql",
            local=True
        )

        del_inactive_products = SparkSQLOperator(
            task_id="delete_inactive_products",
            dag=dag,
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "product_info_table": pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
                },
            sql="template/spark_delete_inactive_products.sql",
            local=True
        )

        update_active_product_info = SparkSQLOperator(
            task_id="update_active_product_info",
            dag=dag,
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "product_info_table": pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
                "columns": list(filter( lambda x: x not in ["execution_date"],
                    pcdefs.PRODUCT_INFO_TABLE.get_columns()
                    ))
                },
            sql="template/spark_update_active_product_info.sql",
            local=True
        )

        active_to_historic_products >> del_inactive_products >> update_active_product_info
    return group
