"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

import os
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator 
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS, SRC_DIR
from src.airflow_tools.dag_defs import TABLE_SETUP as DAG_ID
from src.subdags import table_setup


dag = DAG(
        "testing",
        catchup=False,
        start_date=datetime(2020,6,1),
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )
def _test():
    print("srcdir:", SRC_DIR)
    print("listdir:", os.listdir("/home/airflow/gcs"))
    print("DAGS FOLDER", os.environ.get("DAGS_FOLDER"))
    print("cwd:", os.getcwd())

PythonOperator(
    task_id="print_home",
    dag=dag,
    python_callable=_test
)

BashOperator(
    task_id="dbfs",
    dag=dag,
    bash_command="databricks fs ls"
)

j = {
    "spark_python_task": {
        "python_file": "dbfs:/staging_airflow/spark_scripts/run_sql.py"
    },
    "existing_cluster_id": "0820-181048-frame268"
}

DatabricksSubmitRunOperator(
    task_id="dbrun",
    dag=dag,
    json=j
)
