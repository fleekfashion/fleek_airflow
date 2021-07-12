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
from src.defs.postgre import boards as postboards 
from src.defs.postgre import spark_personalization as persdefs
from src.defs.delta import postgres as spark_defs
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta import boards 
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
                DROP TABLE IF EXISTS {postdefs.NEWLY_INACTIVE_PIDS_TABLE.get_full_name()};
                CREATE TABLE {postdefs.NEWLY_INACTIVE_PIDS_TABLE.get_full_name()} AS 
                SELECT
                    product_id,
                    false as is_active
                    FROM {pinfo_table_name} 
                WHERE is_active AND product_id NOT IN (
                    SELECT product_id 
                    FROM {postdefs.PRODUCT_INFO_TABLE.get_full_name(staging=True)} 
                    ORDER BY product_id);
                UPDATE {pinfo_table_name} pi
                SET is_active=false
                FROM {postdefs.NEWLY_INACTIVE_PIDS_TABLE.get_full_name()} as ni
                WHERE pi.product_id=ni.product_id;
                """
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
            machine_type = "m5d.2xlarge",
            pool_id = None
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

        write_product_color_options_staging = SparkSQLOperator(
            task_id="STAGING_write_product_color_options",
            dag=dag,
            params={
                "src": pcdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_full_name(),
                "target": postdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_delta_name(staging=True),
                "columns": postdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_columns().make_string(", "),
                "mode": "OVERWRITE TABLE",
            },
            sql="template/std_insert.sql",
            local=True,
        )

        write_product_color_options_prod= CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_product_color_options",
            sql=pquery.upsert(
                table_name=postdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_full_name(),
                staging_name=postdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_full_name(staging=True),
                key="product_id, alternate_color_product_id",
                columns=postdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_columns().to_list()
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

        write_smart_tags_staging = SparkSQLOperator(
            task_id="STAGING_write_smart_tags",
            dag=dag,
            params={
                "src": boards.SMART_TAG_TABLE.get_full_name(),
                "target": postboards.SMART_TAG_TABLE.get_delta_name(staging=True),
                "columns": postboards.SMART_TAG_TABLE \
                        .get_columns() \
                        .make_string(", "),
                "mode": "OVERWRITE TABLE"
            },
            sql="template/std_insert.sql",
            local=True
        )

        prod_write_smart_tags = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=persdefs.CONN_ID,
            task_id="PROD_write_smart_tags",
            sql=pquery.upsert(
                table_name=postboards.SMART_TAG_TABLE.get_full_name(),
                staging_name=postboards.SMART_TAG_TABLE.get_full_name(staging=True),
                key="smart_tag_id",
                columns=postboards.SMART_TAG_TABLE.get_columns().to_list(),
            )
        )

        write_product_smart_tags_staging = SparkSQLOperator(
            task_id="STAGING_write_product_smart_tags",
            dag=dag,
            params={
                "src": f"""(
                    SELECT distinct product_id, suggestion_hash as smart_tag_id
                    FROM staging_boards.product_smart_tag
                    WHERE suggestion_hash in (SELECT smart_tag_id FROM staging_boards.smart_tag)
                )
                """,
                "target": postboards.PRODUCT_SMART_TAG_TABLE.get_delta_name(staging=True),
                "columns": postboards.PRODUCT_SMART_TAG_TABLE \
                        .get_columns() \
                        .make_string(", "),
                "mode": "OVERWRITE TABLE"
            },
            sql="template/std_insert.sql",
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
                        first(product_details.product_purchase_url) as product_purchase_url,
                        true AS in_stock
                    FROM t
                    WHERE product_details.size IS NOT NULL
                    GROUP BY product_id, product_details.size
                )""",
                "columns": postdefs.PRODUCT_SIZE_INFO_TABLE.get_columns().make_string(", ")
            },
            local=True,
        )

        prod_write_product_size_info = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_product_size_info",
            sql="template/prod_write_size_info.sql",
            params=dict(
                prod_table=postdefs.PRODUCT_SIZE_INFO_TABLE.get_full_name(),
                staging_table=postdefs.PRODUCT_SIZE_INFO_TABLE.get_full_name(staging=True),
                columns=postdefs.PRODUCT_SIZE_INFO_TABLE.get_columns().make_string(",\n"),
            ),
        )

        prod_write_product_labels = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_product_labels",
            sql="template/prod_write_product_labels.sql",
            params=dict(
                product_info=postdefs.PRODUCT_INFO_TABLE.get_full_name(),
                prod_table=postdefs.PRODUCT_LABELS_TABLE.get_full_name(),
                columns=postdefs.PRODUCT_LABELS_TABLE.get_columns().make_string(",\n"),
                label_column="product_labels"
            ),
        )

        prod_write_secondary_labels = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_secondary_labels",
            sql="template/prod_write_product_labels.sql",
            params=dict(
                product_info=postdefs.PRODUCT_INFO_TABLE.get_full_name(),
                prod_table=postdefs.PRODUCT_SECONDARY_LABELS_TABLE \
                        .get_full_name(),
                columns=postdefs.PRODUCT_SECONDARY_LABELS_TABLE \
                        .get_columns() \
                        .make_string(",\n"),
                label_column="product_secondary_labels"
            ),
        )

        write_advertiser_count_table = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id="PROD_write_advertiser_counts",
            sql=pquery.staging_to_live_query(
                table_name=postdefs.ADVERTISER_PRODUCT_COUNT_TABLE.get_full_name(),
                staging_name=f"""(
                SELECT advertiser_name, count(*) as n_products
                FROM {pinfo_table_name}
                WHERE is_active
                GROUP BY advertiser_name
                ) ac""",
                mode="WRITE_TRUNCATE",
                columns=postdefs.ADVERTISER_PRODUCT_COUNT_TABLE.get_columns().to_list(),
            )
        )


        insert_active_products >> merge_active_products >> [ 
            write_top_products, write_product_price_history, 
            prod_write_product_size_info, write_similar_items_prod,
            write_product_recs_prod, write_advertiser_count_table,
            write_product_color_options_prod, prod_write_product_labels,
            prod_write_secondary_labels,
        ]
        write_similar_items_staging >> write_similar_items_prod
        write_product_recs_staging >> write_product_recs_prod
        insert_product_size_info >> prod_write_product_size_info
        write_product_color_options_staging >> write_product_color_options_prod
        write_smart_tags_staging >> prod_write_smart_tags

    return group 
