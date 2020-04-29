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

from src.defs.bq.schemas import PERSONALIZATION
from src.callable.daily_cj_etl import download_cj_data

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
DATASET = "personalization"
CJ_DOWNLOAD_TABLE_ID = "daily_cj_download"
FULL_CJ_DOWNLOAD_TABLE = ".".join([PROJECT, DATASET, CJ_DOWNLOAD_TABLE_ID])

DAG_ID = "daily_cj_etl"
dag = DAG(
        DAG_ID,
        default_args=DEFAULT_DAG_ARGS,
        schedule_interval=timedelta(days=1)
    )


create_cj_table = BigQueryCreateTableOperator(
        task_id="create_cj_table",
        project_id=PROJECT,
        dataset_id=DATASET,
        table_id=CJ_DOWNLOAD_TABLE_ID,
        schema_fields=PERSONALIZATION[CJ_DOWNLOAD_TABLE_ID],
        dag=dag
        )


parameters = {
    "website-id" : "9089281",
    "advertiser-ids": "joined",
    "keywords": "top",
    "records-per-page": "1000"
    }

cj_data_to_bq = PythonOperator(
        task_id="cj_data_to_bq",
        python_callable=download_cj_data,
        op_kwargs={
            "parameters": parameters,
            "bq_output_table": FULL_CJ_DOWNLOAD_TABLE,
        },
        dag=dag
    )
