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

    with TaskGroup(group_id="product_download", dag=dag) as group:

        truncation = SparkSQLOperator(
            dag=dag,
            task_id=f"truncate_{pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_name()}",
            sql=f"DELETE FROM {pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name()}",
            min_workers=1,
            max_workers=2
        )
        
        with TaskGroup(group_id="cj_api_download", dag=dag) as subgroup:
            parameters: dict = dbfs_read_json(f"{DBFS_DEFS_DIR}/product_download/cj/final_cj_queries.json") # type: ignore
            advertiser_ids = parameters.pop("advertiser_ids")
            for advertiser_id in advertiser_ids:
                query_data = copy.deepcopy(parameters)
                query_data['advertiser_id'] = advertiser_id
                cj_to_delta  = SparkScriptOperator(
                    task_id=f"daily_cj_download_{advertiser_id}",
                    dag=dag,
                    json_args={
                        "query_data": query_data,
                        "output_table": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                    },
                    script="cj_download.py",
                    local=True
                )
            truncation >> subgroup


        with TaskGroup(group_id="cj_partner_download", dag=dag) as subgroup:
            BaseParameters = {
                    "company_id": 5261830,
                    "website_id": 9089281
            }
            PartnerAdvertisers = {
                13830631: "Zaful",
                13276110: "Forever21",
                13237228: "Revolve",
                14463624: "NastyGal"
            }
            for adid, name in PartnerAdvertisers.items():
                cj_to_delta  = SparkScriptOperator(
                    task_id=f"daily_cj_download_{name}",
                    dag=dag,
                    json_args={
                        **BaseParameters,
                        "adid": adid,
                        "output_table": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                    },
                    script="catalog_download.py",
                    local=True
                )
            truncation >> subgroup

        with TaskGroup(group_id="rakuten_download", dag=dag) as subgroup:
            rakuten_advertisers = {
                35719: "ASOS",
                44648: "Princess Polly",
                43177: "Free People",
            }

            for adid, name in rakuten_advertisers.items():
                rakuten_download = SparkScriptOperator(
                    dag=dag,
                    task_id=f"rakuten_download_{name.replace(' ', '_')}",
                    json_args={
                        "adid": adid,
                        "advertiser_name": name,
                        "output_table": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                    },
                    script="rakuten_download.py",
                    init_scripts=["dbfs:/shared/init_scripts/install_xmltodict.sh"],
                    local=True,
                    execution_timeout=timedelta(minutes=20),
                    retries=5
                )

                if name == "ASOS":
                    rakuten_download.machine_type = "m5d.xlarge"
                    rakuten_download.pool_id = None
            truncation >> subgroup

        awin_download = SparkScriptOperator(
            dag=dag,
            task_id=f"awin_download",
            script="awin_download.py",
            sql="template/awin_processing.sql",
            json_args={
                "output_table": pcdefs.DAILY_PRODUCT_DUMP_TABLE.get_full_name(),
                "advertisers": {
                    "Missguided (US & Canada)": "Missguided",
                    "PrettyLittleThing (US)": "PrettyLittleThing",
                },
            },
            params={
                "advertiser_name_map_table": "merchant_name_to_advertiser_name",
                "new_products_table": "new_products"
            },
            local=True,
            execution_timeout=timedelta(minutes=20),
            retries=5
        )
        truncation >> awin_download

    return group
