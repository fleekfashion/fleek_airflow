"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR

def get_operators(dag: DAG_TYPE) -> TaskGroup:
    f"{__doc__}"

    with TaskGroup(group_id="product_info_processing", dag=dag) as group:

        SparkSQLOperator(
            dag=dag,
            task_id="basic_processing",
            json_args={
                "src_table": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                "output_table": pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
                "ds": "{{ds}}",
                "timestamp": "{{ execution_date.int_timestamp }}",
                "drop_kwargs_path": f"{DBFS_DEFS_DIR}/product_download/global/drop_keywords.json" \
                        .replace("dbfs:", "/dbfs"),
                "labels_path": f"{DBFS_DEFS_DIR}/product_download/global/product_labels.json" \
                        .replace("dbfs:", "/dbfs")
            },
            local=True,
        )

        product_info_processing = SparkScriptOperator(
            dag=dag,
            task_id="product_info_processing",
            json_args={
                "src_table": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                "output_table": pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
                "ds": "{{ds}}",
                "timestamp": "{{ execution_date.int_timestamp }}",
                "drop_kwargs_path": f"{DBFS_DEFS_DIR}/product_download/global/drop_keywords.json" \
                        .replace("dbfs:", "/dbfs"),
                "labels_path": f"{DBFS_DEFS_DIR}/product_download/global/product_labels.json" \
                        .replace("dbfs:", "/dbfs")
            },
            script="product_info_processing.py",
            local=True,
            machine_type='i3.xlarge',
            pool_id=None,
            spark_conf={
                'spark.sql.shuffle.partitions': '8'
            }
        )


    return group
