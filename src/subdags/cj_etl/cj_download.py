"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from src.defs.bq import personalization as pdefs
from src.callable.download_cj_graphql import download_cj_data
from src.airflow_tools.airflow_variables import DAG_CONFIG, DAG_TYPE
from src.airflow_tools.operators.bq_safe_truncate_operator import get_safe_truncate_operator

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="cj_download_head", dag=dag)
    tail = DummyOperator(task_id="cj_download_tail", dag=dag)
    
    ## TODO Truncate table before adding to it
    truncations = []
    for table in [ pdefs.DAILY_CJ_DOWNLOAD_TABLE, pdefs.DAILY_NEW_PRODUCT_INFO_TABLE]:
        truncations.append(get_safe_truncate_operator(dag=dag, table=pdefs.get_full_name(table)))

    c = storage.Client(pdefs.PROJECT)
    prefix = DAG_CONFIG["cj_queries_uri_path"]
    blobs = c.list_blobs(bucket_or_name=pdefs.PROJECT,
            prefix=prefix)
    blob = list(blobs)[0]
    parameters = json.loads(blob.download_as_string().decode())
    
    prefix = DAG_CONFIG["cj_drop_kwargs_uri_path"]
    blobs = c.list_blobs(bucket_or_name=pdefs.PROJECT,
            prefix=prefix)
    blob = list(blobs)[0]
    DROP_KWARGS = json.loads(blob.download_as_string().decode())

    advertiser_ids = parameters.pop("advertiser_ids")
    downloads = []
    for advertiser_id in advertiser_ids:
        query_data = copy.deepcopy(parameters)
        query_data['advertiser_id'] = advertiser_id
        cj_data_to_bq = PythonOperator(
            task_id=f"run_daily_cj_download_{advertiser_id}",
            dag=dag,
            python_callable=download_cj_data,
            op_kwargs={
                "query_data": query_data,
                "bq_output_table": pdefs.get_full_name(pdefs.DAILY_CJ_DOWNLOAD_TABLE),
                "drop_kwargs": DROP_KWARGS,
            },
            provide_context=True
        )
        downloads.append(cj_data_to_bq)
    for i in range(1, len(downloads)):
        downloads[i-1] >> downloads[i]

    deduplicate_cj_downloads = BigQueryOperator(
        task_id="deduplicate_cj_downloads",
        dag=dag,
        params={
            "table": pdefs.get_full_name(pdefs.DAILY_CJ_DOWNLOAD_TABLE),
            "columns": ["product_id", "product_name", "execution_timestamp"]
        },
        sql="template/deduplicate_table.sql",
        use_legacy_sql=False
    )

    update_daily_new_product_info_table = BigQueryOperator(
        task_id="update_daily_new_product_info_table",
        dag=dag,
        destination_dataset_table=pdefs.get_full_name(pdefs.DAILY_NEW_PRODUCT_INFO_TABLE),
        write_disposition="WRITE_APPEND",
        params={
            "cj_table": pdefs.get_full_name(pdefs.DAILY_CJ_DOWNLOAD_TABLE),
            "active_table": pdefs.get_full_name(pdefs.ACTIVE_PRODUCTS_TABLE),
            },
        sql="template/update_daily_new_product_info_table.sql",
        use_legacy_sql=False,
        )

    migrate_active_to_historic_products = BigQueryOperator(
        task_id="migrate_active_to_historic_products",
        dag=dag,
        params={
            "active_table": pdefs.get_full_name(pdefs.ACTIVE_PRODUCTS_TABLE),
            "columns": ", ".join(pdefs.get_columns(pdefs.ACTIVE_PRODUCTS_TABLE)),
            "historic_table": pdefs.get_full_name(pdefs.HISTORIC_PRODUCTS_TABLE)
            },
        sql="template/migrate_active_to_historic_products.sql",
        use_legacy_sql=False,
        )
                
    update_active_products = BigQueryOperator(
        task_id="update_active_products",
        dag=dag,
        params={
            "cj_table": pdefs.get_full_name(pdefs.DAILY_CJ_DOWNLOAD_TABLE),
            "active_table": pdefs.get_full_name(pdefs.ACTIVE_PRODUCTS_TABLE),
            "cj_columns": pdefs.get_columns(pdefs.DAILY_CJ_DOWNLOAD_TABLE) 
            },
        sql="template/update_active_products.sql",
        use_legacy_sql=False,
        )

    head >> truncations >> downloads[0] 
    downloads[-1] >> deduplicate_cj_downloads
    deduplicate_cj_downloads >> update_daily_new_product_info_table
    deduplicate_cj_downloads >> migrate_active_to_historic_products


    update_daily_new_product_info_table >> update_active_products
    migrate_active_to_historic_products >> update_active_products
    update_active_products >> tail
    return {"head": head, "tail": tail}
