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
from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator

from src.defs.bq import personalization as pdefs
from src.defs.bq.datasets import PERSONALIZATION as DATASET
from src.callable.daily_cj_etl import download_cj_data

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
FULL_CJ_DOWNLOAD_TABLE = ".".join([PROJECT, DATASET, pdefs.DAILY_CJ_DOWNLOAD_TABLE])
FULL_HISTORIC_PRODUCTS_TABLE = ".".join([PROJECT, DATASET, pdefs.HISTORIC_PRODUCTS_TABLE])
FULL_DAILY_ACTIVE_PRODUCTS_TABLE = ".".join([PROJECT, DATASET, pdefs.DAILY_ACTIVE_PRODUCTS_TABLE])

DAG_ID = "daily_cj_etl_jobs"
dag = DAG(
        DAG_ID,
        default_args=DEFAULT_DAG_ARGS,
        schedule_interval=timedelta(days=1)
    )


create_cj_table = BigQueryCreateTableOperator(
        task_id="create_daily_cj_query_table",
        project_id=PROJECT,
        dataset_id=DATASET,
        table_id=pdefs.DAILY_CJ_DOWNLOAD_TABLE,
        schema_fields=pdefs.SCHEMAS[pdefs.DAILY_CJ_DOWNLOAD_TABLE],
        dag=dag,
        )

create_daily_active_products_table = BigQueryCreateTableOperator(
        task_id="create_daily_active_products_table",
        dag=dag,
        project_id=PROJECT,
        dataset_id=DATASET,
        table_id=pdefs.DAILY_ACTIVE_PRODUCTS_TABLE,
        schema_fields=pdefs.SCHEMAS[pdefs.DAILY_ACTIVE_PRODUCTS_TABLE],
        )

create_historic_products_table = BigQueryCreateTableOperator(
        task_id="create_historic_products_table",
        dag=dag,
        project_id=PROJECT,
        dataset_id=DATASET,
        table_id=pdefs.HISTORIC_PRODUCTS_TABLE,
        schema_fields=pdefs.SCHEMAS[pdefs.HISTORIC_PRODUCTS_TABLE],
        time_partitioning={
            "type" : "DAY",
            "field" : "execution_date"
            }
        )

parameters = {
    "n_pages": 1,
    "product_tag": "none",
    "website-id" : "9089281",
    "advertiser-ids": "joined",
    "keywords": "",
    "records-per-page": "1000",
    }

cj_data_to_bq = PythonOperator(
        task_id="upload_daily_cj_download",
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
            "active_table": FULL_DAILY_ACTIVE_PRODUCTS_TABLE
            },
        sql="template/migrate_active_to_historic_products.sql",
        use_legacy_sql=False,
        )
            

remove_inactive_products = BigQueryOperator(
        task_id="remove_inactive_products",
        dag=dag,
        destination_dataset_table=FULL_DAILY_ACTIVE_PRODUCTS_TABLE,
        write_disposition="WRITE_TRUNCATE",
        params={
            "cj_table": FULL_CJ_DOWNLOAD_TABLE,
            "active_table": FULL_DAILY_ACTIVE_PRODUCTS_TABLE
            },
        sql="template/remove_inactive_products.sql",
        use_legacy_sql=False,
        )




phase1 = [create_cj_table, create_daily_active_products_table,
          create_historic_products_table]
phase1 >> cj_data_to_bq >> migrate_active_to_historic_products
migrate_active_to_historic_products >> remove_inactive_products
