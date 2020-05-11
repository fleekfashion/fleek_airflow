"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from src.defs.bq import personalization as pdefs
from src.callable.daily_cj_etl import download_cj_data

def get_operators(dag):

    head = DummyOperator(task_id="cj_download_head", dag=dag)
    tail = DummyOperator(task_id="cj_download_tail", dag=dag)
    operators = []

    parameters = [{
        "n_pages": 1,
        "product_tag": "none",
        "website-id" : "9089281",
        "advertiser-ids": "joined",
        "keywords": "",
        "records-per-page": "1000",
        }]
    
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
