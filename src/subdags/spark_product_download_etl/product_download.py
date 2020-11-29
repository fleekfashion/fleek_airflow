"""
SubDag to run
Daily CJ Downloads
"""

import json
import copy
from datetime import timedelta

from airflow.operators.dummy_operator import DummyOperator

from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator, dbfs_read_json
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="product_download_head", dag=dag)
    tail = DummyOperator(task_id="product_download_tail", dag=dag)

    truncation = SparkSQLOperator(
        dag=dag,
        task_id=f"truncate_{pcdefs.DAILY_PRODUCT_DUMP_TABLE}",
        sql=f"DELETE FROM {pcdefs.get_full_name(pcdefs.DAILY_PRODUCT_DUMP_TABLE)}",
        min_workers=1,
        max_workers=2
    )

    parameters = dbfs_read_json(f"{DBFS_DEFS_DIR}/product_download/cj/final_cj_queries.json")
    advertiser_ids = parameters.pop("advertiser_ids")
    downloads = []
    for advertiser_id in advertiser_ids:
        query_data = copy.deepcopy(parameters)
        query_data['advertiser_id'] = advertiser_id
        cj_to_delta  = SparkScriptOperator(
            task_id=f"daily_cj_download_{advertiser_id}",
            dag=dag,
            json_args={
                "params": query_data,
                "output_table": pcdefs.get_full_name(pcdefs.DAILY_PRODUCT_DUMP_TABLE),
            },
            script="cj_download.py",
            local=True
        )
        downloads.append(cj_to_delta )
    for i in range(1, len(downloads)):
        downloads[i-1] >> downloads[i]

    ## Add 1 hour timeout for rakuten
    rakuten_download = SparkScriptOperator(
        dag=dag,
        task_id="rakuten_download_products_great_success",
        json_args={
            "valid_advertisers": {
                "ASOS (USA)": "ASOS",
                "NastyGal (US)": "NastyGal",
                "Princess Polly US": "Princess Polly",
                "Topshop": "Topshop",
                "Free People": "Free People"
            },
            "output_table": pcdefs.get_full_name(pcdefs.DAILY_PRODUCT_DUMP_TABLE),
        },
        script="rakuten_download.py",
        init_scripts=["dbfs:/shared/init_scripts/install_xmltodict.sh"],
        local=True,
        execution_timeout=timedelta(hours=1)
    )

    product_info_processing = SparkScriptOperator(
        dag=dag,
        task_id="product_info_processing",
        json_args={
            "src_table": pcdefs.get_full_name(pcdefs.DAILY_PRODUCT_DUMP_TABLE),
            "output_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
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

    process_image_url = SparkSQLOperator(
        dag=dag,
        task_id="process_image_url",
        params={
            "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
        },
        sql="template/process_image_url.sql",
        local=True,
    )

    add_additional_img_urls = SparkSQLOperator(
        dag=dag,
        task_id="add_additional_img_urls",
        params={
            "product_info_table": pcdefs.get_full_name(pcdefs.PRODUCT_INFO_TABLE),
        },
        sql="template/add_additional_image_urls.sql",
        local=True,
    )

    head >> truncation >> [downloads[0], rakuten_download]
    [downloads[-1], rakuten_download] >> product_info_processing >> \
    process_image_url >> add_additional_img_urls >> tail
    return {"head": head, "tail": tail}
