"""
TLDR: Download and update user events

Overview
1. Import data from postgre
2. Run transformations
"""

from datetime import timedelta, date
import copy

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.utils.dates import days_ago

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.databricks.databricks_operators import spark_sql_operator
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.bq import gcs_imports, gcs_exports, user_data, personalization as pdefs
from src.defs.postgre import user_data as postdefs
from src.defs.delta import user_data as delta_user_data
from src.defs.delta import postgres as delta_postgre
from src.subdags import spark_user_events as subdags

DAG_ID = dag_defs.SPARK_USER_EVENTS
DAG_ARGS = copy.copy(DEFAULT_DAG_ARGS)
DAG_ARGS["depends_on_past"] = True

dag = DAG(
        DAG_ID,
        catchup=False,
        max_active_runs=1,
        start_date=days_ago(1),
        schedule_interval="@hourly",
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__,
)

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

postgre_ingestion = subdags.postgre_ingestion.get_operators(dag)
production_stats = subdags.production_stats.get_operators(dag)
 


head >> postgre_ingestion["head"]

postgre_ingestion['tail'] >> production_stats['head']

production_stats["tail"] >> tail
