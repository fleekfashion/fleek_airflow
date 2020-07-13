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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.operators.bq_safe_truncate_operator import get_safe_truncate_operator
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.bq import gcs_imports, gcs_exports, user_data, personalization as pdefs
from src.defs.postgre import personalization as postdefs
from src.defs.postgre import utils as postutils

DAG_ID = dag_defs.USER_EVENTS
DAG_ARGS = copy.copy(DEFAULT_DAG_ARGS)
DAG_ARGS["depends_on_past"] = True

dag = DAG(
        DAG_ID,
        catchup=True,
        max_active_runs=1,
        schedule_interval=timedelta(hours=1),
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__,
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

FILTER = "WHERE event_timestamp < {{ execution_date.int_timestamp }};"
postgre_migrate_to_user_events_export_table = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="postgre_export_user_events_to_staging",
    sql=pquery.export_rows(
        table_name=user_data.USER_EVENTS_TABLE,
        export_table_name=EXPORT_TABLE,
        columns="*",
        delete=True,
        FILTER=FILTER,
    )
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

aggregate_user_events = BigQueryOperator(
    sql="template/aggregate_user_events.sql",
    dag=dag,
    task_id="aggregate_user_events",
    use_legacy_sql=False,
    params={
        "user_events_table": user_data.get_full_name(
            user_data.USER_EVENTS_TABLE
        ),
        "aggregated_events_table": user_data.get_full_name(
            user_data.AGGREGATED_USER_EVENTS_TABLE
        )
    }
)


update_product_stats = BigQueryOperator(
    sql="template/update_product_stats.sql",
    dag=dag,
    task_id="update_product_statistics",
    use_legacy_sql=False,
    params={
        "user_events_table": user_data.get_full_name(
            user_data.USER_EVENTS_TABLE
        ),
        "active_products_table": pdefs.get_full_name(
            pdefs.ACTIVE_PRODUCTS_TABLE
        )
    }
)

head >> postgre_build_user_events_export_table >> postgre_migrate_to_user_events_export_table
postgre_migrate_to_user_events_export_table >> append_user_events

append_user_events >> update_product_stats >> tail
append_user_events >> aggregate_user_events >> tail
def _test():
    print("Sucess")
from airflow.operators.python_operator import PythonOperator
PythonOperator(
    task_id="test",
    dag=dag,
    python_callable=_test
)
