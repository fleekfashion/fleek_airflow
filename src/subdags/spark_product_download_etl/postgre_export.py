"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.databricks.databricks_operators import spark_sql_operator
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.postgre import product_catalog as postdefs
from src.defs.delta import postgres as spark_defs
from src.defs.delta import product_catalog as pcdefs

################################
## PRODUCT TABLE
################################


def get_operators(dag: DAG):
    head = DummyOperator(task_id="postgre_export_head", dag=dag)
    tail = DummyOperator(task_id="postgre_export_tail", dag=dag)

    table_name=postdefs.get_full_name(postdefs.PRODUCT_INFO_TABLE)
    staging_name=postdefs.get_full_name(postdefs.PRODUCT_INFO_TABLE, staging=True)
    columns = ", ".join(
            [ c for c in postdefs.get_columns(postdefs.PRODUCT_INFO_TABLE)
            if "is_active" not in c ])


    insert_active_products = spark_sql_operator(
        task_id="STAGE_active_products",
        dag=dag,
        sql="template/std_insert.sql",
        params={
            "target": spark_defs.get_full_name(postdefs.PRODUCT_INFO_TABLE, staging=True),
            "mode": "OVERWRITE TABLE",
            "src": f"(SELECT *, true as is_active FROM {pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE)})",
            "columns": ", ".join([ c for c in postdefs.get_columns(postdefs.PRODUCT_INFO_TABLE)]),
        },
        min_workers=2,
        max_workers=3
    )

    merge_active_products = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="PROD_merge_active_products",
        sql=pquery.staging_to_live_query(
            table_name=table_name,
            staging_name=staging_name,
            mode="UPSERT",
            key="product_id",
            columns=postdefs.get_columns(postdefs.PRODUCT_INFO_TABLE),
            tail=f""";
                UPDATE {table_name} SET 
                    is_active = false
                WHERE product_id NOT IN (
                    SELECT product_id FROM {staging_name}
                );"""
        )
    )

    write_similar_items_staging= spark_sql_operator(
        task_id="STAGING_write_similar_items",
        dag=dag,
        params={
            "src": pcdefs.get_full_name(pcdefs.SIMILAR_PRODUCTS_TABLE),
            "target": spark_defs.get_full_name(postdefs.SIMILAR_PRODUCTS_TABLE, staging=True),
            "columns": "product_id, similar_product_ids",
            "mode": "OVERWRITE TABLE"
        },
        sql="template/std_insert.sql",
        min_workers=2,
        max_workers=3
    )

    write_similar_items_prod = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="PROD_write_similar_items",
        sql=pquery.staging_to_live_query(
            mode="WRITE_TRUNCATE",
            table_name=postdefs.get_full_name(postdefs.SIMILAR_PRODUCTS_TABLE),
            staging_name=postdefs.get_full_name(postdefs.SIMILAR_PRODUCTS_TABLE, staging=True),
            columns=postdefs.get_columns(postdefs.SIMILAR_PRODUCTS_TABLE),
        )
    )

    head >> insert_active_products >> merge_active_products >> tail
    head >> write_similar_items_staging >> write_similar_items_prod >> tail

    return {"head": head, "tail": tail}
