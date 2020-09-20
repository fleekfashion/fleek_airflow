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
from airflow.utils.dates import days_ago

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.utils import get_dag_sensor
from src.subdags import spark_product_download_etl 

DAG_ID = dag_defs.SPARK_PRODUCT_DOWNLOAD_ETL
dag = DAG(
        DAG_ID,
        catchup=False,
        start_date=days_ago(1),
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

download_operators = spark_product_download_etl.cj_download.get_operators(dag)
product_proc_operators = spark_product_download_etl.product_processing.get_operators(dag)
update_active_prod_operators = spark_product_download_etl.update_active_products \
        .get_operators(dag)
active_products_ml = spark_product_download_etl.active_products_ml.get_operators(dag)

head >> download_operators["head"]

download_operators['tail'] >> product_proc_operators["head"]
download_operators['tail'] >> update_active_prod_operators["head"]

[ product_proc_operators['tail'], update_active_prod_operators['tail'] ] >> \
        active_products_ml["head"]

active_products_ml["tail"] >> tail
