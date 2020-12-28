"""
TLDR: Update product search meili endpoint
"""

from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from functional import seq

from src.airflow_tools.queries import postgre_queries as pquery
from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator
from src.airflow_tools.utils import get_dag_sensor
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta import postgres as phooks
from src.defs.postgre import product_catalog as postdefs
from src.defs import search

DAG_ID = dag_defs.PRODUCT_SEARCH
dag = DAG(
        DAG_ID,
        catchup=False,
        start_date=days_ago(1),
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__
)

trigger = get_dag_sensor(dag, dag_defs.SPARK_PRODUCT_DOWNLOAD_ETL)
head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

upload_products = SparkScriptOperator(
    dag=dag,
    task_id="upload_products",
    script="product_search_upload.py",
    n_workers=0,
    json_args={
        "products_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
        "fields": seq(postdefs.get_columns(postdefs.PRODUCT_INFO_TABLE)) \
                .filter(lambda x: x != "is_active")
                .to_list() + ['swipe_rate'],
        "search_endpoint": f"{pcdefs.PROJECT}_products",
        "search_url": search.URL,
        "search_password": search.PASSWORD
    },
    init_scripts=["install_meilisearch.sh"]
)

trigger >> head >> upload_products >> tail
