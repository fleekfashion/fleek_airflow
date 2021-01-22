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
                "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
                "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
                "columns": ", ".join(pcdefs.get_columns(pcdefs.ACTIVE_PRODUCTS_TABLE)),
                "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE)
                },
            sql="template/spark_active_to_historic.sql",
            local=True
        )

        del_inactive_products = SparkSQLOperator(
            task_id="delete_inactive_products",
            dag=dag,
            params={
                "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
                "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
                },
            sql="template/spark_delete_inactive_products.sql",
            local=True
        )

        process_image_url = SparkSQLOperator(
            dag=dag,
            task_id="process_image_url",
            params={
                "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            },
            sql="template/process_image_url.sql",
            local=True,
        )

        add_additional_image_urls = SparkSQLOperator(
            dag=dag,
            task_id="add_additional_image_urls",
            params={
                "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            },
            sql="template/add_additional_image_urls.sql",
            local=True,
        )

        update_active_product_info = SparkSQLOperator(
            task_id="update_active_product_info",
            dag=dag,
            params={
                "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
                "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
                "columns": list(filter( lambda x: x not in ["execution_date"],
                    pcdefs.get_columns(pcdefs.PRODUCT_INFO_TABLE)
                    ))
                },
            sql="template/spark_update_active_product_info.sql",
            local=True
        )

        process_image_url >> add_additional_image_urls >> update_active_product_info 
        active_to_historic_products >> del_inactive_products
    return group
