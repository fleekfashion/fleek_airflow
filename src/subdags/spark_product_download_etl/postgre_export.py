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
from airflow.utils.task_group import TaskGroup

from src.airflow_tools.databricks.databricks_operators import SparkSQLOperator
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
    with TaskGroup(group_id="postgre_export", dag=dag) as group:

        pinfo_table_name=postdefs.PRODUCT_INFO_TABLE.get_full_name()
        pinfo_staging_name=postdefs.PRODUCT_INFO_TABLE.get_full_name(staging=True)
        columns = postdefs.PRODUCT_INFO_TABLE.get_columns() \
                .filter(lambda x: 'is_active' not in x) \
                .make_string(", ")


        insert_active_products = SparkSQLOperator(
            task_id="STAGE_active_products",
            dag=dag,
            sql="template/std_insert.sql",
            params={
                "target": postdefs.PRODUCT_INFO_TABLE.get_delta_name(staging=True),
                "mode": "OVERWRITE TABLE",
                "src": f"(SELECT *, true as is_active FROM {pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name()})",
                "columns": postdefs.PRODUCT_INFO_TABLE.get_columns().make_string(", ")
            },
            local=True
        )

        insert_product_size_info = SparkSQLOperator(
            task_id="STAGE_product_size_info",
            dag=dag,
            sql="template/std_insert.sql",
            params={
                "target": postdefs.PRODUCT_SIZE_INFO_TABLE.get_delta_name(staging=True),
                "mode": "OVERWRITE TABLE",
                "src": f"""(
                    WITH t AS (
                        SELECT product_id, explode(product_details) AS product_details
                        FROM {pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name()}
                    )
                    SELECT product_id, 
                        product_details.size, 
                        product_details.product_purchase_url, 
                        true AS in_stock
                    FROM t
                )""",
                "columns": postdefs.PRODUCT_SIZE_INFO_TABLE.get_columns().make_string(", ")
            },
            local=True,
            dev_mode=True
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
                columns=postdefs.PRODUCT_INFO_TABLE.get_columns(),
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
                table_name=postdefs.TOP_PRODUCTS_TABLE.get_full_name(),
                staging_name=f"""(
                SELECT *
                FROM {pinfo_table_name}
                WHERE 'top_product'=ANY(product_tags) AND is_active
                ) top_p""",
                mode="WRITE_TRUNCATE",
                columns=postdefs.TOP_PRODUCTS_TABLE.get_columns().to_list(),
            )
        )

        write_product_price_history = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_product_price_history",
            sql=pquery.upsert(
                table_name=postdefs.PRODUCT_PRICE_HISTORY_TABLE.get_full_name(),
                staging_name=f"""(
                SELECT 
                    product_id, 
                    DATE('{{{{ ds }}}}') AS execution_date,
                    product_sale_price AS product_price 
                FROM {pinfo_table_name}
                WHERE is_active
                ) product_prices""",
                key="product_id, execution_date",
                columns=postdefs.PRODUCT_PRICE_HISTORY_TABLE.get_columns().to_list(),
            ),
            trigger_rule='all_done', # Run snapshot regardless of success
        )

        write_similar_items_staging= SparkSQLOperator(
            task_id="STAGING_write_similar_items",
            dag=dag,
            params={
                "src": pcdefs.SIMILAR_PRODUCTS_TABLE.get_full_name(),
                "target": postdefs.SIMILAR_PRODUCTS_TABLE.get_delta_name(staging=True),
                "columns": postdefs.SIMILAR_PRODUCTS_TABLE.get_columns().make_string(", "),
                "mode": "OVERWRITE TABLE",
                "partition_field": "execution_date"
            },
            sql="template/std_partitioned_insert.sql",
            local=True,


        )

        write_similar_items_prod = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_similar_items",
            sql=pquery.upsert(
                table_name=postdefs.SIMILAR_PRODUCTS_TABLE.get_full_name(),
                staging_name=postdefs.SIMILAR_PRODUCTS_TABLE.get_full_name(staging=True),
                key="product_id, index",
                columns=postdefs.SIMILAR_PRODUCTS_TABLE.get_columns().to_list()
            )
        )


        write_product_recs_staging = SparkSQLOperator(
            task_id="STAGING_write_product_recs",
            dag=dag,
            params={
                "src": pdefs.USER_PRODUCT_RECS_TABLE.get_full_name(),
                "target": persdefs.USER_PRODUCT_RECS_TABLE.get_delta_name(staging=True),
                "columns": persdefs.USER_PRODUCT_RECS_TABLE \
                        .get_columns() \
                        .make_string(", "),
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
                table_name=persdefs.USER_PRODUCT_RECS_TABLE.get_full_name(),
                staging_name=persdefs.USER_PRODUCT_RECS_TABLE.get_full_name(staging=True),
                key="user_id, index",
                columns=persdefs.USER_PRODUCT_RECS_TABLE.get_columns().to_list(),
            )
        )

        insert_active_products >> merge_active_products >> [write_top_products, write_product_price_history]
        write_similar_items_staging >> write_similar_items_prod
        write_product_recs_staging >> write_product_recs_prod

    return group 
