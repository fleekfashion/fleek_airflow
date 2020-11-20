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

from src.airflow_tools.databricks.databricks_operators import spark_sql_operator
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.delta import personalization as pdefs
from src.defs.postgre import product_catalog as postdefs
from src.defs.postgre import spark_personalization as persdefs
from src.defs.delta import postgres as spark_defs
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta import postgres as phooks

################################
## PRODUCT TABLE
################################


def get_operators(dag: DAG):
    head = DummyOperator(task_id="postgre_export_head", dag=dag)
    tail = DummyOperator(task_id="postgre_export_tail", dag=dag)

    pinfo_table_name=postdefs.get_full_name(postdefs.PRODUCT_INFO_TABLE)
    pinfo_staging_name=postdefs.get_full_name(postdefs.PRODUCT_INFO_TABLE, staging=True)
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
        local=True
    )

    merge_active_products = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="PROD_merge_active_products",
        sql=pquery.staging_to_live_query(
            table_name=pinfo_table_name,
            staging_name=pinfo_staging_name,
            mode="UPSERT",
            key="product_id",
            columns=postdefs.get_columns(postdefs.PRODUCT_INFO_TABLE),
            tail=f""";
                UPDATE {pinfo_table_name} SET 
                    is_active = false
                WHERE product_id NOT IN (
                    SELECT product_id FROM {pinfo_staging_name}
                );"""
        )
    )

    write_top_products = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="PROD_write_top_products",
        sql=pquery.staging_to_live_query(
            table_name=postdefs.get_full_name(postdefs.TOP_PRODUCTS_TABLE),
            staging_name=f"""(
            SELECT *
            FROM {pinfo_table_name}
            WHERE 'top_product'=ANY(product_tags)
            ) top_p""",
            mode="WRITE_TRUNCATE",
            columns=postdefs.get_columns(postdefs.TOP_PRODUCTS_TABLE),
        )
    )

    write_similar_items_staging= spark_sql_operator(
        task_id="STAGING_write_similar_items",
        dag=dag,
        params={
            "src": pcdefs.get_full_name(pcdefs.SIMILAR_PRODUCTS_TABLE),
            "target": spark_defs.get_full_name(postdefs.SIMILAR_PRODUCTS_TABLE, staging=True),
            "columns": ", ".join(postdefs.get_columns(postdefs.SIMILAR_PRODUCTS_TABLE)),
            "mode": "OVERWRITE TABLE"
        },
        sql="template/std_insert.sql",
        local=True
    )

    write_similar_items_prod = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="PROD_write_similar_items",
        sql=pquery.upsert(
            table_name=postdefs.get_full_name(postdefs.SIMILAR_PRODUCTS_TABLE),
            staging_name=postdefs.get_full_name(postdefs.SIMILAR_PRODUCTS_TABLE, staging=True),
            key="product_id, index",
            columns=postdefs.get_columns(postdefs.SIMILAR_PRODUCTS_TABLE),
        )
    )


    write_product_recs_staging = spark_sql_operator(
        task_id="STAGING_write_product_recs",
        dag=dag,
        params={
            "src": pdefs.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE),
            "target": phooks.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE, staging=True),
            "columns": ", ".join(
                persdefs.get_columns(persdefs.USER_PRODUCT_RECS_TABLE)
            ),
            "mode": "OVERWRITE TABLE"
        },
        sql="template/std_insert.sql",
        local=True
    )

    write_product_recs_prod = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=persdefs.CONN_ID,
        task_id="PROD_write_product_recs",
        sql=pquery.upsert(
            table_name=persdefs.get_full_name(persdefs.USER_PRODUCT_RECS_TABLE),
            staging_name=persdefs.get_full_name(persdefs.USER_PRODUCT_RECS_TABLE, staging=True),
            key="user_id, index",
            columns=persdefs.get_columns(persdefs.USER_PRODUCT_RECS_TABLE),
        )
    )

    head >> insert_active_products >> merge_active_products >> write_top_products >> tail
    head >> write_similar_items_staging >> write_similar_items_prod >> tail
    head >> write_product_recs_staging >> write_product_recs_prod >> tail

    return {"head": head, "tail": tail}
