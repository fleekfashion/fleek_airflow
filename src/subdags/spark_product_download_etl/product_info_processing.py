"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy
from datetime import timedelta
from functional import seq

from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR, PROJECT

TABLE1 = f"{PROJECT}_tmp.product_info_processing_step_1"
TABLE2 = f"{PROJECT}_tmp.product_info_processing_step_2"

DROP_KWARGS_PATH = f"{DBFS_DEFS_DIR}/product_download/global/drop_keywords.json"
LABELS_PATH = f"{DBFS_DEFS_DIR}/product_download/global/product_labels.json"
LABELS = dbfs_read_json(LABELS_PATH)

def _process_args(args):
    new_args = {}
    for key, value in args.items():
        if len(value) > 0:
            new_args[key] = value
    return new_args


def _build_filter_string(args: dict):
    and_filters = []
    for field, local_args in args.items():
        LOCAL_FILTERS = []
        if type(local_args) == str:
            local_args = [local_args]
        if field in ["product_labels", "product_secondary_labels", "product_external_labels", "product_tags"]:
            field = f"concat_ws(' && ', {field})"
        for local_arg in local_args:
            local_arg = local_arg.lower().replace('\\', '\\\\')
            LOCAL_FILTERS.append(f"lower({field}) rLIKE '{local_arg}'")
        local_filter = " OR ".join(LOCAL_FILTERS)
        local_filter = f"({local_filter}) AND {field} IS NOT NULL"
        and_filters.append(local_filter)
    f = "\n\tAND\n".join(map(lambda x: f"({x})", and_filters))
    return f

def args_to_filter(args):
    args = _process_args(args)
    EXCLUDE = args.get("EXCLUDE")

    args.pop("EXCLUDE") if EXCLUDE else None
    EXCLUDE = _process_args(EXCLUDE) if EXCLUDE else {}
    exclude_filter = _build_filter_string(EXCLUDE) if len(EXCLUDE) > 0 else None

    if exclude_filter:
        return f"({_build_filter_string(args)} \n\tAND NOT\n {exclude_filter} )"
    else:
        return _build_filter_string(args)

def get_operators(dag: DAG_TYPE) -> TaskGroup:
    f"{__doc__}"

    with TaskGroup(group_id="product_info_processing", dag=dag) as group:

        basic_processing = SparkSQLOperator(
            dag=dag,
            sql="template/basic_product_info_processing.sql",
            task_id="basic_processing",
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


        label_filters = dict()
        update_sets = []
        for key, value in LABELS.items():
            label_filters[key] = seq(value).map(args_to_filter).make_string("\n\nOR\n\n")
        res = [ [k, v] for k, v in label_filters.items() ]

        kwargs_processing = SparkSQLOperator(
            dag=dag,
            sql="template/product_kwarg_processing.sql",
            task_id="kwargs_processing",
            params={
                "src": TABLE1,
                "output": TABLE2,
                "label_filters": label_filters,
                "updates": res

            },
            output_table=TABLE2,
            mode="WRITE_TRUNCATE",
            options={
                "overwriteSchema": "true"
            },
            dev_mode=True
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
