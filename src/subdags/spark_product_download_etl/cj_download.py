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
from src.defs.delta.utils import SHARED_POOL_ID, DBFS_DEFS_DIR

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"
    head = DummyOperator(task_id="cj_download_head", dag=dag)
    tail = DummyOperator(task_id="cj_download_tail", dag=dag)

    truncation = spark_sql_operator(
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

    head >> truncation >> downloads[0] 
    downloads[-1] >> tail 
    return {"head": head, "tail": tail}
