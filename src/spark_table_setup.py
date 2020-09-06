"""
TLDR: Set up Spark and Postgre Tables for All Dags.

Overview
1. Create tables in schema files
2. Alter spark tables if schema changes
Note: Does not alter postgre tables
"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools.dag_defs import SPARK_TABLE_SETUP as DAG_ID
from src.subdags import spark_table_setup

dag = DAG(
        DAG_ID,
        start_date=days_ago(1),
        schedule_interval="@weekly",
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

delta_operators = spark_table_setup.delta.get_operators(dag)

head >> [ 
    delta_operators['head']
]

[ 
    delta_operators['tail'], 
] >> tail
