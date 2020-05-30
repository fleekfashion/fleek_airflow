"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.subdags import table_setup
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceDatabaseCreateOperator

DAG_ID = "testing"
dag = DAG(
        DAG_ID,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

GCP_PROJECT_ID = "fleek-prod"
INSTANCE_NAME = "fleek-app-prod1"
DB_NAME = "testdb"
REGION = "us-central1"
USER = "postgres"
TEST = 'test'


db_create_body = {
    "instance": INSTANCE_NAME,
    "name": DB_NAME,
    "project": GCP_PROJECT_ID
}


sql_db_create_task2 = CloudSqlInstanceDatabaseCreateOperator(
    dag=dag,
    body=db_create_body,
    instance=INSTANCE_NAME,
    task_id='sql_db_create_task2'
)
