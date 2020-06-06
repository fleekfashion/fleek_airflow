"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

import os
from datetime import timedelta

import numpy as np
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceDatabaseCreateOperator, CloudSqlQueryOperator, CloudSqlInstanceImportOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools.operators import cloudql_operators as csql
from src.subdags import table_setup
from src.defs.bq import personalization as pdefs
from src.defs.postgre import utils as postutils
from src.defs.postgre import personalization as postdefs

GCP_PROJECT_ID = "fleek-prod"
INSTANCE_NAME = "fleek-app-prod1"
DB_NAME = "ktest"
REGION = "us-central1"
USER = "postgres"
TEST = 'test'
################################
## PRODUCT TABLE
################################


def get_operators(dag):
    head = DummyOperator(task_id="postgre_export_head", dag=dag)
    tail = DummyOperator(task_id="postgre_export_tail", dag=dag)

    TABLE_NAME = pdefs.FULL_NAMES[pdefs.ACTIVE_PRODUCTS_TABLE] 
    DEST = "fleek-prod.gcs_exports.postgre_product_info"

    cols = ["product_id",
            "advertiser_name",
            "product_purchase_url",
            "product_description",
            "product_name",
            #"product_brand",
            "product_price",
            "product_sale_price",
            "product_image_url",
            #"product_additional_image_urls",
            "product_tag",
            "n_views",
            "n_likes",
            "n_add_to_cart",
            "n_conversions"
            ]

    last_col = cols[-1]
    parameters = {
        "prod_table": TABLE_NAME,
        "cols": cols[:-1],
        "last_col": last_col
    }

    SQL = """
    SELECT {% for col in params.cols %} 
        {{col}}, {% endfor %}
        {{ params.last_col }}
    FROM {{ params.prod_table }}
    """

    prod_info_bq_export = BigQueryOperator(
        dag=dag,
        task_id=f"prod_info_to_gcs_exports",
        sql=SQL,
        params=parameters,
        destination_dataset_table=DEST,
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )


    POSTGRE_PTABLE = "product_info"
    GCS_DEST = "gs://fleek-prod/personalization/postgre_upload/product_info"
    prods_bq_to_gcs = BigQueryToCloudStorageOperator(
        dag=dag,
        task_id="prod_bq_to_gcs",
        source_project_dataset_table=DEST,
        destination_cloud_storage_uris=GCS_DEST,
        export_format="CSV",
        print_header=False

    )

    col_info = []
    for c in pdefs.SCHEMAS[pdefs.ACTIVE_PRODUCTS_TABLE]:
        if c["name"] in cols:
            col_info.append ( { 
                "name": c["name"], 
                "type": postutils.BQ_TO_PG[c["type"]], 
                "mode": postutils.BQ_TO_PG[c["mode"]]
            }
            )
    tail = f";\nCREATE INDEX ON {POSTGRE_PTABLE} (product_id)"

    postgre_build_product_table = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=CONN_ID,
        task_id="build_postgres_product_info_table",
        sql=postutils.create_table_query(
            table_name=POSTGRE_PTABLE,
            columns=col_info,
            tail=tail,
            drop=True,
        )
    )

    staging_name = POSTGRE_PTABLE + "_staging"
    postgre_build_product_staging_table = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=CONN_ID,
        task_id="build_postgres_product_info_staging_table",
        sql=postutils.create_staging_table_query(
            table_name=POSTGRE_PTABLE,
            denomer="_staging"
        )
    )

    product_data_import = csql.get_import_operator(
        dag=dag,
        task_id="postgre_import_product_data",
        uri=GCS_DEST,
        database="ktest",
        table=staging_name,
        instance=INSTANCE_NAME,
        columns=cols,
    )

    product_info_staging_to_prod = CloudSqlQueryOperator(
        dag=dag,
        gcp_cloudsql_conn_id=CONN_ID,
        task_id="postgres_product_info_staging_to_live",
        sql=postutils.staging_to_live_query(
            table_name=POSTGRE_PTABLE,
            staging_name=staging_name,
            mode="UPDATE_APPEND",
            key="product_id",
        )
    )

    prod_info_bq_export >> prods_bq_to_gcs >> postgre_build_product_table 
    postgre_build_product_table >> postgre_build_product_staging_table >> product_data_import >> product_info_staging_to_prod
