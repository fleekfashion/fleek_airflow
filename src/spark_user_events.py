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


FILTER = "WHERE event_timestamp < {{ execution_date.int_timestamp }};"
postgre_export_user_events_to_staging = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="postgre_export_user_events_to_staging",
    sql=pquery.export_rows(
        table_name=postdefs.get_full_name(postdefs.USER_EVENTS_TABLE),
        export_table_name=postdefs.get_full_name(postdefs.USER_EVENTS_TABLE, staging=True),
        columns="*",
        delete=True,
        clear_export_table=True,
        FILTER=FILTER,
    )
)

append_user_events_func = lambda project_output_table: spark_sql_operator(
    sql="""
    SELECT 
        *, 
        DATE(from_unixtime(event_timestamp, 'yyyy-MM-dd')) as execution_date,
        {{ execution_date.int_timestamp }} as airflow_execution_timestamp
    FROM {{params.SRC}}""",
    dag=dag,
    task_id=f"append_user_events_project_{project_output_table[0]}",
    params={
        "SRC": delta_postgre.get_full_name(postdefs.USER_EVENTS_TABLE),
    },
    mode="WRITE_APPEND",
    output_table=project_output_table[1],
    local=True
)


main_user_events_table = delta_user_data.get_full_name(delta_user_data.USER_EVENTS_TABLE)
if delta_user_data.PROJECT == "staging":
    secondary_project = "prod"
    secondary_user_events_table = main_user_events_table.replace("staging", "prod")
else:
    secondary_project = "staging"
    secondary_user_events_table = main_user_events_table.replace("prod", "staging")

append_user_events = append_user_events_func((delta_user_data.PROJECT, main_user_events_table))
append_user_events_secondary = append_user_events_func((secondary_project, secondary_user_events_table))
 


head >> postgre_export_user_events_to_staging
postgre_export_user_events_to_staging >> [ append_user_events, append_user_events_secondary]

append_user_events >> tail
