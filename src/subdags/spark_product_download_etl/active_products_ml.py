"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import personalization as pdefs
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR
from src.defs.delta import user_data as user_delta
from src.defs.delta import boards
from src.defs.postgre import static

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"

    with TaskGroup(group_id="product_ml", dag=dag) as group:
        compute_product_similarity = SparkSQLOperator(
            task_id="compute_product_similarity",
            dag=dag,
            sql="template/compute_product_similarity.sql",
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "historic_table": pcdefs.HISTORIC_PRODUCTS_TABLE.get_full_name(),
                "output_table": pcdefs.PRODUCT_SIMILARITY_SCORES_TABLE.get_full_name(),
                "historic_days": 45,
                "min_score": .6
            },
            output_table=pcdefs.PRODUCT_SIMILARITY_SCORES_TABLE.get_full_name(),
            mode="WRITE_TRUNCATE",
            drop_duplicates=True,
            duplicates_subset=["product_id", "similar_product_id"],
            machine_type = "m5d.2xlarge",
            pool_id = None,
            local=True,
            dev_mode=False
        )

        process_similar_products = SparkScriptOperator(
            task_id="process_similar_products",
            dag=dag,
            script="process_similar_products.py",
            sql="template/similar_items_processing.sql",
            json_args={
                "output_table": "{{params.processed_similarity_table}}",
                "TOP_N": 100,
                "DS": "{{ds}}"
            },
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "historic_table": pcdefs.HISTORIC_PRODUCTS_TABLE.get_full_name(),
                "product_similarity_table": pcdefs.PRODUCT_SIMILARITY_SCORES_TABLE.get_full_name(),
                "processed_similarity_table": pcdefs.SIMILAR_PRODUCTS_TABLE.get_full_name(),
            },
            num_workers=4,
            dev_mode=False
        )

        generate_product_color_options = SparkSQLOperator(
            task_id="generate_product_color_options",
            dag=dag,
            sql="template/generate_product_color_options.sql",
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
            },
            output_table=pcdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_full_name(),
            mode="WRITE_TRUNCATE",
            drop_duplicates=True,
            duplicates_subset=["product_id", "alternate_color_product_id"],
            local=True,
            options={
                "overwriteSchema": "true"
            },
        )

        product_recs = SparkSQLOperator(
            task_id="product_recommendations",
            dag=dag,
            sql="template/product_recs_template.sql",
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "events_table": user_delta.USER_EVENTS_TABLE.get_full_name(),
                "n_days": 90
            },
            output_table=pdefs.USER_PRODUCT_RECS_TABLE.get_full_name(),
            mode="WRITE_TRUNCATE",
            local=True,
        )

        ##############################################
        ## PRODUCT TAGS
        ##############################################

        daily_top_product_tag = SparkSQLOperator(
            task_id="daily_top_product_tag",
            dag=dag,
            sql="template/daily_top_product_tag.sql",
            params={
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
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
                "active_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "n_days": 3,
                "tag": "new_product"
            },
            local=True
        )

        product_smart_tags = SparkSQLOperator(
            task_id="product_smart_tags",
            dag=dag,
            sql="template/build_product_smart_tags.sql",
            params={
                "active_products_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "synonyms_table": static.SYNONYMS_TABLE.get_delta_name(),
            },
            local=True,
            output_table=boards.PRODUCT_SMART_TAG_TABLE.get_full_name(),
            options={
                "overwriteSchema": "true"
            },
            mode="WRITE_TRUNCATE"
        )

        smart_tags = SparkSQLOperator(
            task_id="smart_tags",
            dag=dag,
            sql="template/build_smart_tags.sql",
            params={
                "product_smart_tag_table": boards.PRODUCT_SMART_TAG_TABLE.get_full_name(),
                "min_include": 75
            },
            local=True,
            output_table=boards.SMART_TAG_TABLE.get_full_name(),
            options={
                "overwriteSchema": "true" 
            },
            drop_duplicates=True,
            duplicates_subset=['smart_tag_id'],
            mode="WRITE_TRUNCATE"
        )
        

        advertiser_top_smart_tags = SparkSQLOperator(
            task_id="advertiser_top_smart_tags",
            dag=dag,
            sql="template/advertiser_top_smart_tags.sql",
            params={
                "active_products_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
                "product_smart_tag_table": boards.PRODUCT_SMART_TAG_TABLE.get_full_name(),
                "smart_tag_table": boards.SMART_TAG_TABLE.get_full_name(),
                "min_c": 10,
                "min_brand_pct": .01,
                "rank_cutoff": 50,
                "pl_rank_cutoff": 50
            },
            local=True,
            output_table=boards.ADVERTISER_SMART_TAGS.get_full_name(),
            options={
                "overwriteSchema": "true" 
            },
            drop_duplicates=True,
            duplicates_subset=['smart_tag_id', 'advertiser_name'],
            mode="WRITE_TRUNCATE"
        )

        compute_product_similarity >> process_similar_products
        product_recs
        generate_product_color_options
        daily_top_product_tag >> daily_new_product_tag
        product_smart_tags >> smart_tags >> advertiser_top_smart_tags
    return group
