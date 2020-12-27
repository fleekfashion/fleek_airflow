"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import personalization as pdefs
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR
from src.defs.delta import user_data as user_delta

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="active_products_ml_head", dag=dag)
    tail = DummyOperator(task_id="active_products_ml_tail", dag=dag)

    compute_product_similarity = SparkSQLOperator(
        task_id="compute_product_similarity",
        dag=dag,
        sql="template/compute_product_similarity.sql",
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE),
            "historic_days": 45,
            "min_score": .4
        },
        output_table=pcdefs.get_full_name(pcdefs.PRODUCT_SIMILARITY_SCORES),
        mode="WRITE_TRUNCATE",
        drop_duplicates=True,
        duplicates_subset=["product_id", "similar_product_id"],
        num_workers=8,
    )

    process_similar_products = SparkScriptOperator(
        task_id="process_similar_products",
        dag=dag,
        script="process_similar_products.py",
        sql="template/similar_items_processing.sql",
        json_args={
            "output_table": "{{params.processed_similarity_table}}",
            "TOP_N": 100,
        },
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE),
            "product_similarity_table": pcdefs.get_full_name(pcdefs.PRODUCT_SIMILARITY_SCORES),
            "processed_similarity_table": pcdefs.get_full_name(pcdefs.SIMILAR_PRODUCTS_TABLE),
        },
        num_workers=4,
    )

    product_recs = SparkScriptOperator(
        task_id="product_recommendations",
        dag=dag,
        script="product_recs.py",
        json_args={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "historic_table": pcdefs.get_full_name(pcdefs.HISTORIC_PRODUCTS_TABLE),
            "events_table": user_delta.get_full_name(user_delta.USER_EVENTS_TABLE),
            "output_table": pdefs.get_full_name(pdefs.USER_PRODUCT_RECS_TABLE),
            "TOP_N": 500
        },
        num_workers=4,
    )

    ##############################################
    ## PRODUCT TAGS
    ##############################################

    daily_top_product_tag = SparkSQLOperator(
        task_id="daily_top_product_tag",
        dag=dag,
        sql="template/daily_top_product_tag.sql",
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "min_views": 2,
            "limit": 1000,
            "tag": "top_product"
        },
        local=True
    )

    daily_new_product_tag = SparkSQLOperator(
        task_id="daily_new_product_tag",
        dag=dag,
        sql="template/daily_new_product_tag.sql",
        params={
            "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
            "n_days": 3,
            "tag": "new_product"
        },
        local=True
    )

    head >> compute_product_similarity >> process_similar_products >> tail
    head >> product_recs >> tail
    head >> daily_top_product_tag >> daily_new_product_tag >> tail
    return {"head": head, "tail": tail}
