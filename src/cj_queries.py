"""
TLDR: Download and update active_products.

Overview
1. Download data from data streams
2. Run new products through embedding model
3. Migrate old product to historical
4. Update active products
"""

from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.utils import get_dag_sensor
from src.subdags import cj_etl

DAG_ID = dag_defs.DATA_DOWNLOAD_ETL
dag = DAG(
        DAG_ID,
        catchup=False,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

download_operators = cj_etl.cj_download.get_operators(dag)
embeddings_operators = cj_etl.new_product_embeddings.get_operators(dag)
postgre_export_operators = cj_etl.postgre_export.get_operators(dag)

head >> download_operators["head"]
download_operators["tail"] >> embeddings_operators["head"]
embeddings_operators["tail"] >> postgre_export_operators["head"]
postgre_export_operators['tail'] >> tail

def _test():
    print("Sucess")
from airflow.operators.python_operator import PythonOperator
PythonOperator(
    task_id="test",
    dag=dag,
    python_callable=_test
)
