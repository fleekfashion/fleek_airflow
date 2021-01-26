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
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, PROJECT

TABLE1 = f"{PROJECT}_tmp.product_info_processing_step_1"
TABLE2 = f"{PROJECT}_tmp.product_info_processing_step_2"

def get_operators(dag: DAG_TYPE) -> TaskGroup:
    f"{__doc__}"

    with TaskGroup(group_id="product_info_processing", dag=dag) as group:

        step1 = SparkSQLOperator(
            dag=dag,
            sql="template/basic_product_info_processing.sql",
            task_id="step1_basic",
            params={
                "src": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                "unchanged_columns": pcdefs.PRODUCT_INFO_TABLE.get_columns() \
                        .filter(
                            lambda x: x not in [
                                "product_id", "execution_date", "execution_timestamp",
                                "product_tags",
                                "product_labels", "product_secondary_labels",
                                "product_external_labels", "product_sale_price"
                        ]
                    ).make_string(", "),
                "required_fields_filter": pcdefs.PRODUCT_INFO_TABLE.get_fields() \
                        .filter(lambda x: x.nullable) \
                        .map(lambda x: f"{x.name} is NOT NULL") \
                        .make_string(" AND "),
                "ungrouped_columns": pcdefs.PRODUCT_INFO_TABLE.get_columns() \
                        .filter(
                            lambda x: x not in [
                                "product_id"
                        ]
                    ).map(
                        lambda x: f"first({x}) as {x}"
                    ).make_string(", "),
            },
            dev_mode=True,
            output_table=TABLE1,
            mode="WRITE_TRUNCATE",
        )


        stepn = SparkSQLOperator(
            dag=dag,
            sql="template/basic_product_info_processing.sql",
            task_id="write_to_product_info",
            params={
                "src": TABLE1,
                "output": TABLE2,
                "ungrouped_columns": pcdefs.PRODUCT_INFO_TABLE.get_columns() \
                        .filter(
                            lambda x: x not in [
                                "product_id"
                        ]
                    ).map(
                        lambda x: f"first({x}) as {x}"
                    ).make_string(", "),
            },
            dev_mode=True,
            output_table=TABLE2,
            mode="WRITE_TRUNCATE",
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
