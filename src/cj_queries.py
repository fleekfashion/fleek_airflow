"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools.utils import get_table_setup_sensor
from src.subdags import cj_etl

DAG_ID = "daily_cj_etl_jobs"
dag = DAG(
        DAG_ID,
        default_args=DEFAULT_DAG_ARGS,
        schedule_interval=timedelta(days=1),
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)
table_setup_sensor = get_table_setup_sensor(dag)

download_operators = cj_etl.cj_download.get_operators(dag)
embeddings_operators = cj_etl.new_product_embeddings.get_operators(dag)

head >> table_setup_sensor >> download_operators["head"]
download_operators["tail"] >> embeddings_operators["head"]
embeddings_operators['tail'] >> tail

def _test():
    print("Sucess")
from airflow.operators.python_operator import PythonOperator
PythonOperator(task_id="test", dag=dag, python_callable=_test)
