"""
TLDR: Make Product Recommendations 

Overview
"""

from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.utils import get_dag_sensor
from src.subdags import product_recommendations as subdags

DAG_ID = dag_defs.PRODUCT_RECOMMENDATIONS
dag = DAG(
        DAG_ID,
        catchup=False,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)
table_setup_sensor = get_dag_sensor(dag=dag, external_dag_id=dag_defs.TABLE_SETUP)
table_setup_sensor >> head

rec_operators = subdags.recommender.get_operators(dag)
postgre_upload_operators = subdags.postgre_export.get_operators(dag)

head >> rec_operators["head"]
rec_operators["tail"] >> postgre_upload_operators["head"]
postgre_upload_operators["tail"] >> tail

def _test():
    print("Sucess")

from airflow.operators.python_operator import PythonOperator
PythonOperator(
    task_id="test",
    dag=dag,
    python_callable=_test
)
