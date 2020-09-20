"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy

from google.cloud import storage

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, spark_sql_operator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import GENERAL_CLUSTER_ID, SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="new_product_processing_head", dag=dag)
    tail = DummyOperator(task_id="new_product_processing_tail", dag=dag)
    IMG_DIR = f"{DBFS_AIRFLOW_DIR}/data/product_images/{{{{ds}}}}".replace("dbfs:", "/dbfs")

    product_info_processing = SparkScriptOperator(
        dag=dag,
        task_id="product_info_processing",
        json_args={
            "src_table": pcdefs.get_full_name(pcdefs.DAILY_PRODUCT_DUMP_TABLE),
            "output_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            "ds": "{{ds}}",
            "timestamp": "{{ execution_date.int_timestamp }}",
            "drop_kwargs_path": f"{DBFS_DEFS_DIR}/product_download/global/drop_keywords.json" \
                    .replace("dbfs:", "/dbfs")
        },
        script="product_info_processing.py",
        cluster_id=GENERAL_CLUSTER_ID
    )

    image_download = SparkScriptOperator(
        dag=dag,
        task_id="image_download",
        script="download_images.py",
        json_args={
            "ds": "{{ds}}",
            "outpath": IMG_DIR,
            "src_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            "active_products_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE)
        },
        min_workers=1,
        max_workers=2,
    )

    new_product_ml = SparkScriptOperator(
        dag=dag,
        task_id="new_product_ml",
        script="product_ml.py",
        json_args={
            "img_dir": "/staging/airflow/data/product_images/{{ds}}/",
            "model_path": "/dbfs/ml/models/product_image_embeddings/inception/",
            "version": "2",
            "dest_table": pcdefs.get_full_name(pcdefs.NEW_PRODUCT_FEATURES_TABLE)
        },
        local=True,
        machine_type="p2.xlarge",
        pool_id=None,
        init_scripts=["dbfs:/shared/init_scripts/install_opencv.sh"]
    )

    append_new_products = spark_sql_operator(
        task_id="apend_new_products",
        dag=dag,
        params={
            "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
            "prod_ml_features_table": pcdefs.get_full_name(pcdefs.NEW_PRODUCT_FEATURES_TABLE),
            },
        sql="template/spark_append_new_active_products.sql",
        output_table=pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
        mode="WRITE_APPEND",
        min_workers=1,
        max_workers=2
    )

    head >> product_info_processing >> image_download >> new_product_ml
    new_product_ml >> append_new_products >> tail


    return {"head": head, "tail": tail}
