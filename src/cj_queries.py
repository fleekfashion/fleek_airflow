"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS

from src.defs.bq import personalization as pdefs
from src.defs.bq import gcs_exports as g_exports
from src.defs.bq.datasets import PERSONALIZATION as DATASET, GCS_EXPORTS
from src.callable.daily_cj_etl import download_cj_data
from src.subdags import cj_etl

DAG_ID = "daily_cj_etl_jobs"
dag = DAG(
        DAG_ID,
        default_args=DEFAULT_DAG_ARGS,
        schedule_interval=timedelta(days=1),
    )

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
FULL_CJ_DOWNLOAD_TABLE = ".".join([PROJECT, DATASET, pdefs.DAILY_CJ_DOWNLOAD_TABLE])
FULL_HISTORIC_PRODUCTS_TABLE = ".".join([PROJECT, DATASET, pdefs.HISTORIC_PRODUCTS_TABLE])
FULL_ACTIVE_PRODUCTS_TABLE = ".".join([PROJECT, DATASET, pdefs.ACTIVE_PRODUCTS_TABLE])
FULL_DAILY_NEW_PRODUCT_INFO_TABLE = ".".join([PROJECT, DATASET, pdefs.DAILY_NEW_PRODUCT_INFO_TABLE])
FULL_SAGEMAKER_EMBEDDER_PRODUCT_INFO = ".".join(
        [
            PROJECT,
            GCS_EXPORTS,
            g_exports.SAGEMAKER_EMBEDDER_PRODUCT_INFO
        ]
    )


table_setup_operators = cj_etl.table_setup.get_operators(dag)

parameters = {
    "n_pages": 1,
    "product_tag": "none",
    "website-id" : "9089281",
    "advertiser-ids": "joined",
    "keywords": "",
    "records-per-page": "1000",
    }

cj_data_to_bq = PythonOperator(
        task_id="save_daily_cj_download",
        dag=dag,
        python_callable=download_cj_data,
        op_kwargs={
            "parameters": parameters,
            "bq_output_table": FULL_CJ_DOWNLOAD_TABLE,
        },
        provide_context=True
    )

migrate_active_to_historic_products = BigQueryOperator(
        task_id="migrate_active_to_historic_products",
        dag=dag,
        destination_dataset_table=FULL_HISTORIC_PRODUCTS_TABLE,
        write_disposition="WRITE_APPEND",
        params={
            "cj_table": FULL_CJ_DOWNLOAD_TABLE,
            "active_table": FULL_ACTIVE_PRODUCTS_TABLE
            },
        sql="template/migrate_active_to_historic_products.sql",
        use_legacy_sql=False,
        )
            

remove_inactive_products = BigQueryOperator(
        task_id="remove_inactive_products",
        dag=dag,
        destination_dataset_table=FULL_ACTIVE_PRODUCTS_TABLE,
        write_disposition="WRITE_TRUNCATE",
        params={
            "cj_table": FULL_CJ_DOWNLOAD_TABLE,
            "active_table": FULL_ACTIVE_PRODUCTS_TABLE
            },
        sql="template/remove_inactive_products.sql",
        use_legacy_sql=False,
        )

update_daily_new_product_info_table = BigQueryOperator(
        task_id="update_daily_new_product_info_table",
        dag=dag,
        destination_dataset_table=FULL_DAILY_NEW_PRODUCT_INFO_TABLE,
        write_disposition="WRITE_TRUNCATE",
        params={
            "cj_table": FULL_CJ_DOWNLOAD_TABLE,
            "active_table": FULL_ACTIVE_PRODUCTS_TABLE,
            },
        sql="template/update_daily_new_product_info_table.sql",
        use_legacy_sql=False,
        )

update_sagemaker_embedder_product_info_export = BigQueryOperator(
        task_id="update_sagemaker_embedder_product_info_export",
        dag=dag,
        destination_dataset_table=FULL_SAGEMAKER_EMBEDDER_PRODUCT_INFO,
        write_disposition="WRITE_TRUNCATE",
        params={
            "product_info_table": FULL_DAILY_NEW_PRODUCT_INFO_TABLE,
            },
        sql="template/update_sagemaker_embedder_product_info_export.sql",
        use_legacy_sql=False,
        )





table_setup_operators >> cj_data_to_bq >> migrate_active_to_historic_products
migrate_active_to_historic_products >> remove_inactive_products >> update_daily_new_product_info_table
update_daily_new_product_info_table >> update_sagemaker_embedder_product_info_export 

