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
LABELS_TABLE = f"{PROJECT}_tmp.product_labels"
IMAGE_URL_TABLE = f"{PROJECT}_tmp.combined_table"
ADDITIONAL_IMAGE_URL_TABLE = f"{PROJECT}_tmp.combined_table"
COMBINED_TABLE = f"{PROJECT}_tmp.combined_table"

DROP_KWARGS_PATH = f"{DBFS_DEFS_DIR}/product_download/global/drop_keywords.json"
LABELS_PATH = f"{DBFS_DEFS_DIR}/product_download/global/product_labels.json"
LABELS = dbfs_read_json(LABELS_PATH)
DROP_KWARGS = dbfs_read_json(DROP_KWARGS_PATH)

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
                                "product_external_labels", "product_sale_price",
                                "product_additional_image_urls"
                        ]
                    ).make_string(", "),
                "required_fields_filter": pcdefs.PRODUCT_INFO_TABLE.get_fields() \
                        .filter(lambda x: not x.nullable) \
                        .filter(lambda x: x.name != "product_details") \
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
            options={
                "overwriteSchema": "true"
            },
        )

        label_filters = dict()
        for key, value in LABELS.items():
            label_filters[key] = seq(value).map(args_to_filter).make_string("\n\nOR\n\n")
        res = [ [k, v] for k, v in label_filters.items() ]
        apply_product_labels = SparkSQLOperator(
            dag=dag,
            sql="template/apply_product_labels.sql",
            task_id="apply_product_labels",
            params={
                "src": TABLE1,
                "output": LABELS_TABLE,
                "label_filters": label_filters,
                "updates": res

            },
            output_table=LABELS_TABLE,
            mode="WRITE_TRUNCATE",
            options={
                "overwriteSchema": "true"
            },
            dev_mode=True
        )

        process_image_urls = SparkSQLOperator(
            dag=dag,
            task_id="process_image_url",
            params={
                "product_info_table": TABLE1,
            },
            sql="template/process_image_url.sql",
            output_table=IMAGE_URL_TABLE,
            mode="WRITE_TRUNCATE",
            options={
                "overwriteSchema": "true"
            },
            dev_mode=True
        )

        add_additional_image_urls = SparkSQLOperator(
            dag=dag,
            task_id="add_additional_image_urls",
            params={
                "product_info_table": TABLE1 ,
                "image_urls_table": IMAGE_URL_TABLE,
                "labels": LABELS_TABLE,
            },
            sql="template/add_additional_image_urls.sql",
            output_table=ADDITIONAL_IMAGE_URL_TABLE,
            mode="WRITE_TRUNCATE",
            options={
                "overwriteSchema": "true"
            },
            dev_mode=True
        )

        combine_info = SparkSQLOperator(
            dag=dag,
            sql="template/combine_product_info.sql",
            task_id="combine_product_info",
            params={
                "labels": LABELS_TABLE,
                "image_url_table": IMAGE_URL_TABLE,
                "additional_image_urls_table": ADDITIONAL_IMAGE_URL_TABLE,
                "src": TABLE1,
                "drop_args_filter": seq(DROP_KWARGS) \
                        .map(args_to_filter) \
                        .make_string("\n\nOR\n\n"),
                "columns": pcdefs.PRODUCT_INFO_TABLE.get_columns() \
                        .filter(lambda x: x not in [
                            "product_id", "product_labels",
                            "product_image_url", "product_additional_image_urls"
                        ]) \
                        .make_string(", ")
            },
            output_table=COMBINED_TABLE,
            mode="WRITE_TRUNCATE",
            options={
                "overwriteSchema": "true"
            },
            dev_mode=True
        )
        
        write_to_product_info = SparkSQLOperator(
            dag=dag,
            sql="template/product_info_grouping.sql",
            task_id="write_to_product_info",
            params={
                "src": COMBINED_TABLE,
                "ungrouped_columns": pcdefs.PRODUCT_INFO_TABLE.get_columns() \
                        .filter(
                            lambda x: x not in [
                                "product_id", "product_details"
                        ]
                    ).map(
                        lambda x: f"first({x}) as {x}"
                    ).make_string(", "),
            },
            dev_mode=True,
            output_table=pcdefs.PRODUCT_INFO_TABLE.get_full_name(),
            options={
                "replaceWhere":"execution_date = '{{ds}}'",
                "mergeSchema": "true"
            },
            mode="WRITE_TRUNCATE",
        )

        basic_processing >> [ apply_product_labels, process_image_urls] >> \
                add_additional_image_urls >> combine_info >> \
                write_to_product_info 
    return group
