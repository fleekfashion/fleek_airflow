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
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, spark_sql_operator
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

product_recs = SparkScriptOperator(
    task_id="product_recommendations",
    dag=dag,
    script="product_recs.py",
    json_args={
        "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
        "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE),
        "events_table": user_delta.get_full_name(user_delta.USER_EVENTS_TABLE),
        "output_table": pdefs.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE),
        "TOP_N": 500
    },
    num_workers=4,
)

write_product_recs_staging = spark_sql_operator(
    task_id="STAGING_write_product_recs",
    dag=dag,
    params={
        "src": pdefs.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE),
        "target": phooks.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE, staging=True),
        "columns": ", ".join([
            "product_id",
            "posexplode(top_product_ids) AS (`index`, product_id)"
        ]),
        "mode": "OVERWRITE TABLE"
    },
    sql="template/std_insert.sql",
    local=True
)

write_product_recs_prod = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="PROD_write_product_recs",
    sql=pquery.upsert(
        table_name=postdefs.get_full_name(postdefs.USER_PRODUCT_RECS_TABLE),
        staging_name=postdefs.get_full_name(postdefs.USER_PRODUCT_RECS_TABLE, staging=True),
        key="user_id, index",
        columns=postdefs.get_columns(postdefs.USER_PRODUCT_RECS_TABLE),
    )
)

head >> product_recs >> write_product_recs_staging >> write_product_recs_prod >> tail
