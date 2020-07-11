"""
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
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from src.airflow_tools.airflow_variables import DAG_CONFIG
from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.bq import gcs_imports, gcs_exports, personalization as pdefs
from src.defs.postgre import utils as postutils
from src.defs.postgre import personalization as postdefs

TABLE_NAME = gcs_imports.get_full_name(gcs_imports.USER_PRODUCT_RECOMMENDATIONS_TABLE)
DEST = gcs_exports.get_full_name(gcs_exports.USER_RECOMMENDATIONS_TABLE)
TOP_N = DAG_CONFIG.get("model_parameters").get(
    "product_recommender").get(
    "top_n")
GCS_DEST = "gs://fleek-prod/personalization/postgre_upload/product_recs"

def get_operators(dag: DAG):
    head = DummyOperator(task_id="postgre_export_head", dag=dag)
    dag_tail = DummyOperator(task_id="postgre_export_tail", dag=dag)

    del_rec_table = BigQueryTableDeleteOperator(
        task_id=f"delete_bq_postgre_export_rec_table",
        dag=dag,
        deletion_dataset_table=DEST,
        ignore_if_missing=True,
    )

    TOP_N = 2000
    ## TODO Convert this to a single SQL query via unions
    batch_size = 50
    batches = [b for b in range(0, TOP_N, batch_size)]
    batch_params = []
    for batch in range(1, len(batches)):
        inds = list(enumerate(range(batches[batch-1], batches[batch])))
        batch_params.append({
            "batch": batch,
            "inds": inds,
        })

    build_product_recs_export_table = BigQueryOperator(
        dag=dag,
        task_id=f"build_product_recs_export_table",
        sql="template/build_product_recs_export_table.sql",
        params={
            "batch_params": batch_params,
            "rec_table": TABLE_NAME
        },
        destination_dataset_table=DEST,
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
    )

    recs_bq_to_gcs = BigQueryToCloudStorageOperator(
        dag=dag,
        task_id="recs_bq_to_gcs",
        source_project_dataset_table=DEST,
        destination_cloud_storage_uris=GCS_DEST,
        export_format="CSV",
        print_header=False
    )

    TABLE = postdefs.USER_RECOMMENDATIONS_TABLE
    STAGING_TABLE = TABLE + postutils.DENOMER
    columns = [
        "user_id bigint NOT NULL",
        "batch integer NOT NULL"
    ]
    for b in range(batch_size):
        columns.append(f"top_products_{b} bigint NOT NULL")

    tail = f"""PARTITION BY LIST(batch);
    CREATE INDEX ON {STAGING_TABLE} (user_id);
    """
    for b in range(len(batches)):
        tail += f"CREATE TABLE IF NOT EXISTS {STAGING_TABLE}_{b} PARTITION OF {STAGING_TABLE} FOR VALUES IN ({b});\n"

    postgre_build_rec_table = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="build_postgres_user_product_recs_staging_table",
        sql=pquery.create_table_query(
            STAGING_TABLE,
            columns,
            tail=tail,
            drop=True
        ),
    )
        
    data_import = csql.get_import_operator(
        dag=dag,
        task_id="postgre_import_rec_data_to_staging",
        uri=GCS_DEST,
        database="ktest",
        table=STAGING_TABLE,
        instance=postdefs.INSTANCE
    )

    tail = ""
    for b in range(len(batches)):
        tail += f"ALTER TABLE {STAGING_TABLE}_{b} RENAME TO {TABLE}_{b};\n"

    postgre_rec_table_staging_to_prod = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="postgres_user_product_recs_staging_to_prod",
        sql=pquery.staging_to_live_query(
            staging_name=STAGING_TABLE,
            table_name=TABLE,
            tail=tail,
            mode="REPLACE_TABLE"
        ),
    )

    sql = f"""
    BEGIN;
    TRUNCATE {postdefs.USER_BATCH_TABLE};
    INSERT INTO {postdefs.USER_BATCH_TABLE} ( user_id, batch, last_filter) 
    SELECT 
        DISTINCT ON
        (user_id) user_id,
        1 as batch, 
        '' as last_filter
    FROM {postdefs.USER_RECOMMENDATIONS_TABLE} ur
    WHERE ur.batch=1;
    END;
    """
    update_batch_table = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=postdefs.CONN_ID,
        task_id="update_batch_table",
        sql=sql
    )

    head >> del_rec_table >> build_product_recs_export_table
    build_product_recs_export_table >> recs_bq_to_gcs >> postgre_build_rec_table
    postgre_build_rec_table >> data_import >> postgre_rec_table_staging_to_prod
    postgre_rec_table_staging_to_prod >> update_batch_table >> dag_tail
    return {"head": head, "tail": dag_tail}
