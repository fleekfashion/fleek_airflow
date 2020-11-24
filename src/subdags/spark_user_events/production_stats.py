"""
TLDR: Ingest new user_events from postgre into delta
Write to both prod and staging tables
"""

from datetime import timedelta, date
import copy

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.databricks.databricks_operators import SparkSQLOperator
from src.defs.postgre import user_data as postdefs
from src.defs.delta import user_data as userdefs 
from src.defs.delta import product_catalog as pcdefs 

def get_operators(dag: DAG) -> dict:
    head = DummyOperator(task_id=f"production_stats_head", dag=dag)
    tail = DummyOperator(task_id=f"production_stats_tail", dag=dag)

    update_product_stats = SparkSQLOperator(
        dag=dag,
        task_id="update_product_stats",
        sql="template/spark_update_product_stats.sql",
        params={
            "src": userdefs.get_full_name(userdefs.USER_EVENTS_TABLE),
            "target": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE)
        },
        local=True
    )

    head >> update_product_stats >> tail

    return {"head": head, "tail": tail}

