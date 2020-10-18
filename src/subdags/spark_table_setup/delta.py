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
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

from src.defs.delta.utils import DBFS_SCRIPT_DIR
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, spark_sql_operator, create_table_operator
from src.defs import delta

databases = [delta.product_catalog, delta.user_data]

def get_operators(dag: DAG):
    head = DummyOperator(task_id="delta_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="delta_table_setup_tail", dag=dag)

    for db in databases:
        for table, info in db.TABLES.items():
            op = create_table_operator(
                task_id=f"create_table_{table}",
                dag=dag,
                table=db.get_full_name(table),
                schema=info["schema"],
                partition=info.get("partition"),
                comment=info.get("comment"),
                local=True
            )
            head >> op >> tail
    return {"head": head, "tail": tail}
