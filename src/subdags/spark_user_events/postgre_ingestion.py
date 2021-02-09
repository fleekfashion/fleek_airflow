"""
TLDR: Ingest new user_events from postgre into delta
Write to both prod and staging tables
"""

from datetime import timedelta, date
import copy

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator

from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.databricks.databricks_operators import SparkSQLOperator
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.utils import PROJECT
from src.defs.postgre import user_data as postdefs
from src.defs.delta import user_data as delta_user_data
from src.defs.delta import postgres as delta_postgre

def get_operators(dag: DAG) -> dict:
    head = DummyOperator(task_id=f"postgre_ingestion_head", dag=dag)
    tail = DummyOperator(task_id=f"postgre_ingestion_tail", dag=dag)


    FILTER = "WHERE event_timestamp < {{ execution_date.int_timestamp }};"
    postgre_export_user_events_to_staging = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="postgre_export_user_events_to_staging",
        sql=pquery.export_rows(
            table_name=postdefs.USER_EVENTS_TABLE.get_full_name(),
            export_table_name=postdefs.USER_EVENTS_TABLE.get_full_name(staging=True),
            columns="*",
            delete=True,
            clear_export_table=True,
            FILTER=FILTER,
        )
    )

    append_user_events_func = lambda project_output_table: SparkSQLOperator(
        sql="""
        SELECT 
            *, 
            DATE(from_unixtime(event_timestamp, 'yyyy-MM-dd')) as execution_date,
            cast({{ execution_date.int_timestamp }} as bigint) as airflow_execution_timestamp
        FROM {{params.SRC}}""",
        dag=dag,
        task_id=f"append_user_events_project_{project_output_table[0]}",
        params={
            "SRC": delta_postgre.get_full_name(postdefs.USER_EVENTS_TABLE_NAME),
        },
        mode="WRITE_APPEND",
        output_table=project_output_table[1],
        local=True
    )


    main_user_events_table = delta_user_data.USER_EVENTS_TABLE.get_full_name()
    if PROJECT == "staging":
        secondary_project = "prod"
        secondary_user_events_table = main_user_events_table.replace("staging", "prod")
    else:
        secondary_project = "staging"
        secondary_user_events_table = main_user_events_table.replace("prod", "staging")

    append_user_events = append_user_events_func((PROJECT, main_user_events_table))
    append_user_events_secondary = append_user_events_func((secondary_project, secondary_user_events_table))
     


    head >> postgre_export_user_events_to_staging
    postgre_export_user_events_to_staging >> [ append_user_events, append_user_events_secondary]
    append_user_events >> tail

    return {"head": head, "tail": tail}
