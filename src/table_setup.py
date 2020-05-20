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
from src.airflow_tools.dag_defs import TABLE_SETUP as DAG_ID
from src.subdags import table_setup

dag = DAG(
        DAG_ID,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

p_tables = table_setup.personalization.get_operators(dag)
g_exp_tables = table_setup.gcs_exports.get_operators(dag)
g_imp_tables = table_setup.gcs_imports.get_operators(dag)

head >> [ p_tables['head'], g_exp_tables['head'], g_imp_tables['head'] ]
[ p_tables['tail'], g_exp_tables['tail'], g_exp_tables['tail'] ] >> tail
