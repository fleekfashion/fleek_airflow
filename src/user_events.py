"""
TLDR: Download and update user events

Overview
1. Import data from postgre
2. Run transformations
"""

from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.bq import gcs_imports, gcs_exports, user_data, personalization as pdefs
from src.defs.postgre import personalization as postdefs
from src.defs.postgre import utils as postutils

DAG_ID = dag_defs.USER_EVENTS
dag = DAG(
        DAG_ID,
        catchup=False,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__
    )

head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

EXPORT_TABLE = postdefs.USER_EVENTS_TABLE + "_export"

postgre_build_user_events_export_table = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id=f"build_postgres_{postdefs.USER_EVENTS_TABLE}_export_table",
    sql=pquery.create_staging_table_query(
        table_name=postdefs.USER_EVENTS_TABLE,
        denomer="_export"
    )
)


FILTER = "WHERE event_timestamp < {{ execution_date.int_timestamp }}"
SQL = f"""
BEGIN TRANSACTION;

INSERT INTO {EXPORT_TABLE}
SELECT * FROM {postdefs.USER_EVENTS_TABLE}
{FILTER};

DELETE FROM {postdefs.USER_EVENTS_TABLE}
{FILTER};

END TRANSACTION;
"""

postgre_migrate_to_user_events_export_table = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="postgre_export_user_events_to_staging",
    sql=SQL
)


append_user_events = BigQueryOperator(
    sql="template/append_user_events.sql",
    dag=dag,
    task_id="append_user_events",
    use_legacy_sql=False,
    params={
        "user_events_table": user_data.get_full_name(
            user_data.USER_EVENTS_TABLE
        ),
        "cloud_sql_export_table": EXPORT_TABLE,
        "external_conn_id": postdefs.BQ_EXTERNAL_CONN_ID,
        "columns": ", ".join(
            postdefs.get_columns(
                postdefs.USER_EVENTS_TABLE
            )
        )
    }
)

head >> postgre_build_user_events_export_table >> postgre_migrate_to_user_events_export_table
postgre_migrate_to_user_events_export_table >> append_user_events >> tail

def _test():
    print("Sucess")
from airflow.operators.python_operator import PythonOperator
PythonOperator(
    task_id="test",
    dag=dag,
    python_callable=_test
)
