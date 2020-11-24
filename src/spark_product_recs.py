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
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator

from src.airflow_tools.queries import postgre_queries as pquery
from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator
from src.airflow_tools.utils import get_dag_sensor
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta import user_data as user_delta
from src.defs.delta import personalization as pdefs
from src.defs.delta import postgres as phooks
from src.defs.postgre import spark_personalization as postdefs

DAG_ID = dag_defs.SPARK_PRODUCT_RECOMMENDATIONS
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

