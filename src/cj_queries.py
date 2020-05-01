"""
DAG to run queries to CJ
and download the data to a 
daily BQ table.
"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator

from src.defs.bq.personalization import DAILY_CJ_DOWNLOAD_TABLE, DAILY_CJ_DOWNLOAD_TABLE_SCHEMA
from src.defs.bq.datasets import PERSONALIZATION as DATASET
from src.callable.daily_cj_etl import download_cj_data

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
FULL_CJ_DOWNLOAD_TABLE = ".".join([PROJECT, DATASET, DAILY_CJ_DOWNLOAD_TABLE])

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
        table_id=DAILY_CJ_DOWNLOAD_TABLE,
        schema_fields=DAILY_CJ_DOWNLOAD_TABLE_SCHEMA,
        dag=dag,
        time_partitioning={
            "type" : "DAY",
            "field" : "execution_date_timestamp"
            }
        )


parameters = {
    "website-id" : "9089281",
    "advertiser-ids": "joined",
    "keywords": "",
    "records-per-page": "1000"
    }

cj_data_to_bq = PythonOperator(
        task_id="upload_daily_cj_download",
        python_callable=download_cj_data,
        op_kwargs={
            "parameters": parameters,
            "bq_output_table": FULL_CJ_DOWNLOAD_TABLE,
        },
        dag=dag,
        provide_context=True
    )

create_cj_table >> cj_data_to_bq
