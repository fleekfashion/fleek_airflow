"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.subdags import table_setup
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceDatabaseCreateOperator, CloudSqlDatabaseHook, CloudSqlQueryOperator
from airflow import secrets

DAG_ID = "testing"
dag = DAG(
        DAG_ID,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

GCP_PROJECT_ID = "fleek-prod"
INSTANCE_NAME = "fleek-app-prod1"
DB_NAME = "ktest"
REGION = "us-central1"
USER = "postgres"
TEST = 'test'

db_create_body = {
    "instance": INSTANCE_NAME,
    "name": DB_NAME,
    "project": GCP_PROJECT_ID
}


conn_id = 'google_cloud_sql_ktest'

sql_db_create_task2 = CloudSqlInstanceDatabaseCreateOperator(
    dag=dag,
    gcp_cloudsql_conn_id=conn_id,
    body=db_create_body,
    instance=INSTANCE_NAME,
    task_id='sql_db_create_task2'
)

hook = CloudSqlDatabaseHook(
    gcp_cloudsql_conn_id=conn_id,
    gcp_conn_id="google_cloud_default",
    default_gcp_project_id="fleek-prod")

sql_db_test_query = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=conn_id,
    task_id="test_query",
    sql="""
    INSERT INTO test_users (user_id, product0, batch) VALUES (12,12,0);
    """
)
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.models.connection import * 

def get_conn():
    conn_id = 'google_cloud_sql_test'
    
    c = Connection(conn_id=conn_id, conn_type="gcpcloudsql")
    print("GOT CONN", c, c.login, c.password, c.host)

    hook = c.get_hook()
    print("hook_vals", hook.user, hook.password, hook.public_ip)
    
    from airflow import secrets
    cons = secrets.get_connections(conn_id)
    print(cons)
    print("conn_host", cons[0].host)

    e = EnvironmentVariablesBackend()
    uri = e.get_conn_uri(conn_id)
    print("GET ENV CONN URI", uri)
    conn = e.get_connections(conn_id)[0]
    print("GOT ENV CONN", conn.login, conn.password, conn.host)
    print("ENV CONN", conn.conn_type)
    
    parsed = urlparse(uri)
    print(uri, "PARSED", parsed)
    ctype = urlparse(uri).scheme
    print("CTYPE", ctype)

    sc = secrets.get_connections(conn_id)
    print(sc)

    for secrets_backend in secrets.ensure_secrets_loaded():
        conn_list = secrets_backend.get_connections(conn_id=conn_id)
        print("cl", conn_list, "backend", secrets_backend)

    hook = CloudSqlDatabaseHook(
        gcp_cloudsql_conn_id=conn_id,
        gcp_conn_id="google_cloud_default",
        default_gcp_project_id="fleek-prod")

    conn = hook.get_connection(conn_id)
    print("GOT CONN", conn.login, conn.password, conn.host)

    print("hook_vals", hook.user, hook.password, hook.public_ip)

    hook.validate_ssl_certs()
    hook.create_connection()
    hook.delete_connection()

from airflow.operators.python_operator import PythonOperator
PythonOperator(
    task_id="test", 
    dag=dag, 
    python_callable=get_conn
)

sql_db_create_task2 >> sql_db_test_query

