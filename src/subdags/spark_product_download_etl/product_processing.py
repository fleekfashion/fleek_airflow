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
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, DBFS_AIRFLOW_DIR

def get_operators(dag: DAG_TYPE) -> TaskGroup:
    f"{__doc__}"
    IMG_TABLE = f"{pcdefs.PROJECT}_tmp.daily_image_download" 

    with TaskGroup(group_id="new_product_processing", dag=dag) as group:
        image_download = SparkScriptOperator(
            dag=dag,
            task_id="image_download",
            script="download_images.py",
            json_args={
                "ds": "{{ds}}",
                "output_table": IMG_TABLE,
                "src_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
                "active_products_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE)
            },
            num_workers=3
        )

        new_product_ml = SparkScriptOperator(
            dag=dag,
            task_id="new_product_ml",
            script="product_ml.py",
            json_args={
                "img_table": IMG_TABLE,
                "model_path": "/dbfs/ml/models/product_image_embeddings/inception/",
                "version": "2",
                "dest_table": pcdefs.get_full_name(pcdefs.NEW_PRODUCT_FEATURES_TABLE),
                "num_partitions": 1
            },
            pool_id=None,
            machine_type="p2.xlarge",
            local=True,
            spark_version="7.2.x-gpu-ml-scala2.12",
            init_scripts=["dbfs:/shared/init_scripts/install_opencv.sh"]
        )

        append_new_products = SparkSQLOperator(
            task_id="apend_new_products",
            dag=dag,
            params={
                "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
                "prod_ml_features_table": pcdefs.get_full_name(pcdefs.NEW_PRODUCT_FEATURES_TABLE),
                "active_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
                "columns": ", ".join(pcdefs.get_columns(pcdefs.ACTIVE_PRODUCTS_TABLE)),
                },
            sql="template/spark_append_new_active_products.sql",
            local=True,
        )

        image_download >> new_product_ml >> append_new_products
    return group
