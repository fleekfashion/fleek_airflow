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
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS, SRC_DIR
from src.airflow_tools.dag_defs import DATABRICKS_SETUP as DAG_ID
from src.defs.delta.utils import DBFS_SCRIPT_DIR
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, create_table_operator


dag = DAG(
        DAG_ID,
        catchup=False,
        start_date=days_ago(1),
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

