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
from src.airflow_tools.databricks.databricks_operators import run_custom_spark_job, DatabricksSQLOperator


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

op2 = run_custom_spark_job(
    task_id="dbrun",
    dag=dag,
    script="run_sql.py",
    parameters=["--sql=SELECT * FROM test.wow"],
    cluster_id=GENERAL_CLUSTER_ID,
    min_workers=2,
    max_workers=3
)

op3 = DatabricksSQLOperator(
    dag=dag,
    task_id="op_test",
    sql="SELECT * FROM test.wow",
    cluster_id=GENERAL_CLUSTER_ID
)

from pyspark.sql.types import *
schema1= StructType([
  StructField(name="a", dataType=IntegerType(), nullable=False, metadata={"comment": "comment", "default": 0})
]
)

schema2 = StructType([
  StructField(name="a", dataType=IntegerType(), nullable=False, metadata={"comment": "comment", "default": 0}),
  StructField(name="b", dataType=IntegerType(), nullable=False, metadata={"comment": "comment", "default":0}),
   StructField(name="c", dataType=IntegerType(), nullable=True, metadata={"comment": "comment"})
]
).json()

create_table = run_custom_spark_job(
    task_id="create_table",
    dag=dag,
    script="create_table.py",
    parameters=["--table=test.airflow", f"--schema={schema2}"],
    cluster_id=GENERAL_CLUSTER_ID,
)

op1 >> op2
