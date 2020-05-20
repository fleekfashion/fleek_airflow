"""
SubDag to run
Daily CJ Downloads
"""

import json

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from src.defs.bq import personalization as pdefs
from src.callable.daily_cj_etl import download_cj_data
from src.airflow_tools.airflow_variables import DAG_CONFIG, DAG_TYPE

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="cj_download_head", dag=dag)
    tail = DummyOperator(task_id="cj_download_tail", dag=dag)
    operators = []
    
    c = storage.Client(pdefs.PROJECT)
    prefix = DAG_CONFIG["cj_queries_uri_path"]

    blobs = c.list_blobs(bucket_or_name=pdefs.PROJECT,
            prefix=prefix)
    blob = list(blobs)[0]
    parameters = json.loads(blob.download_as_string().decode())
    parameters = parameters["queries"]
    
    downloads = []
    for p in parameters:
        cj_data_to_bq = PythonOperator(
            task_id="run_daily_cj_download",
            dag=dag,
            python_callable=download_cj_data,
            op_kwargs={
                "parameters": p,
                "bq_output_table": pdefs.FULL_NAMES[pdefs.DAILY_CJ_DOWNLOAD_TABLE],
            },
            provide_context=True
        )
        downloads.append(cj_data_to_bq)

    update_daily_new_product_info_table = BigQueryOperator(
        task_id="update_daily_new_product_info_table",
        dag=dag,
        destination_dataset_table=pdefs.FULL_NAMES[pdefs.DAILY_NEW_PRODUCT_INFO_TABLE],
        write_disposition="WRITE_APPEND",
        params={
            "cj_table": pdefs.FULL_NAMES[pdefs.DAILY_CJ_DOWNLOAD_TABLE],
            "active_table": pdefs.FULL_NAMES[pdefs.ACTIVE_PRODUCTS_TABLE],
            },
        sql="template/update_daily_new_product_info_table.sql",
        use_legacy_sql=False,
        )

    migrate_active_to_historic_products = BigQueryOperator(
        task_id="migrate_active_to_historic_products",
        dag=dag,
        destination_dataset_table=pdefs.FULL_NAMES[pdefs.HISTORIC_PRODUCTS_TABLE],
        write_disposition="WRITE_APPEND",
        params={
            "cj_table": pdefs.FULL_NAMES[pdefs.DAILY_CJ_DOWNLOAD_TABLE],
            "active_table": pdefs.FULL_NAMES[pdefs.ACTIVE_PRODUCTS_TABLE]
            },
        sql="template/migrate_active_to_historic_products.sql",
        use_legacy_sql=False,
        )
                
    remove_inactive_products = BigQueryOperator(
        task_id="remove_inactive_products",
        dag=dag,
        destination_dataset_table=pdefs.FULL_NAMES[pdefs.ACTIVE_PRODUCTS_TABLE],
        write_disposition="WRITE_TRUNCATE",
        params={
            "cj_table": pdefs.FULL_NAMES[pdefs.DAILY_CJ_DOWNLOAD_TABLE],
            "active_table": pdefs.FULL_NAMES[pdefs.ACTIVE_PRODUCTS_TABLE]
            },
        sql="template/remove_inactive_products.sql",
        use_legacy_sql=False,
        )

    downloads >> update_daily_new_product_info_table
    downloads >> migrate_active_to_historic_products

    operators.extend(downloads)
    operators.append(update_daily_new_product_info_table)
    operators.append(migrate_active_to_historic_products)

    operators >> remove_inactive_products 
    operators.append(remove_inactive_products)

    head >> operators >> tail
    return {"head": head, "tail": tail}
