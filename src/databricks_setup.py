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
from src.defs.delta.utils import DBFS_SCRIPT_DIR, GENERAL_CLUSTER_ID


dag = DAG(
        DAG_ID,
        catchup=False,
        start_date=days_ago(1),
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

op1 = BashOperator(
    task_id="dbfs",
    dag=dag,
    bash_command=f"dbfs cp -r --overwrite {SRC_DIR}/spark_scripts {DBFS_SCRIPT_DIR}"
)

j = {
    "spark_python_task": {
        "python_file": f"{DBFS_SCRIPT_DIR}/run_sql.py",
        "parameters": [
            "--sql=SELECT * FROM test.wow"
            ]
    },
    "existing_cluster_id": GENERAL_CLUSTER_ID
}

op2 = DatabricksSubmitRunOperator(
    task_id="dbrun",
    dag=dag,
    json=j
)

op1 >> op2
