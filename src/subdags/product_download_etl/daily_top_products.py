"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.bq import gcs_imports, gcs_exports, personalization as pdefs
from src.defs.postgre import personalization as postdefs
from src.defs.postgre import utils as postutils

################################
## PRODUCT TABLE
################################


def get_operators(dag: DAG):
    head = DummyOperator(task_id="daily_top_products_head", dag=dag)
    dag_tail = DummyOperator(task_id="daily_top_products_tail", dag=dag)
