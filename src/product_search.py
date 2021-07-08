"""
TLDR: Update product search meili endpoint
"""

from datetime import timedelta
from typing import Dict, List
import copy

import meilisearch
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor 
from airflow.utils.dates import days_ago
from functional import seq

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator
from src.airflow_tools.utils import get_dag_sensor
from src.callable import search_settings, upload_trending_documents
from src.defs.delta import product_catalog as pcdefs
from src.defs.postgre import product_catalog as postdefs
from src.defs.postgre import static
from src.defs.delta.utils import DBFS_DEFS_DIR, LARGE_CLUSTER_ID
from src.defs import search

DAG_ID = dag_defs.PRODUCT_SEARCH
DAG_ARGS = copy.copy(DEFAULT_DAG_ARGS)
dag = DAG(
        DAG_ID,
        catchup=False,
        start_date=days_ago(1),
        max_active_runs=1,
        schedule_interval="@daily",
        default_args=DAG_ARGS,
        description=__doc__
)

trigger = get_dag_sensor(
    dag,
    dag_defs.SPARK_PRODUCT_DOWNLOAD_ETL,
    timeout=timedelta(hours=6)
)
head = DummyOperator(task_id=f"{DAG_ID}_dag_head", dag=dag)
tail = DummyOperator(task_id=f"{DAG_ID}_dag_tail", dag=dag)

AUTOCOMPLETE_DEFS_DIR = f"{DBFS_DEFS_DIR}/search/autocomplete"
PRODUCT_SEARCH_DEFS_DIR = f"{DBFS_DEFS_DIR}/search/products"

update_product_search_settings = PythonOperator(
    task_id="update_product_search_settings",
    dag=dag,
    python_callable=search_settings.update_settings,
    op_kwargs={
        "synonyms_filepath": f"{DBFS_DEFS_DIR}/search/global/synonyms.json",
        "settings_filepath": f"{PRODUCT_SEARCH_DEFS_DIR}/settings.json",
        "index_name": search.PRODUCT_SEARCH_INDEX
    }
)

upload_products = SparkScriptOperator(
    dag=dag,
    task_id="upload_products",
    sql="template/process_product_search_products.sql",
    script="product_search_upload.py",
    local=True,
    json_args={
        "fields": postdefs.PRODUCT_INFO_TABLE.get_columns() \
                .filter(lambda x: x != "is_active")
                .to_list() + [
                    'swipe_rate', 'default_search_order',
                    'sizes', 'product_color_options'
                ],
        "search_endpoint": search.PRODUCT_SEARCH_INDEX,
        "search_url": search.URL,
        "search_password": search.PASSWORD
    },
    params={
        "active_products_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
        "color_options_table": pcdefs.PRODUCT_COLOR_OPTIONS_TABLE.get_full_name(),
    },
    init_scripts=["install_meilisearch.sh"]
)

update_autocomplete_settings = PythonOperator(
    task_id="update_autocomplete_settings",
    dag=dag,
    python_callable=search_settings.update_settings,
    op_kwargs={
        "synonyms_filepath": f"{DBFS_DEFS_DIR}/search/global/synonyms.json",
        "settings_filepath": f"{AUTOCOMPLETE_DEFS_DIR}/settings.json",
        "index_name": search.AUTOCOMPLETE_INDEX
    }
)

AUTOCOMPLETE_DEFS_LOCAL_DIR = AUTOCOMPLETE_DEFS_DIR.replace('dbfs:/', '/dbfs/')
autocomplete_upload = SparkScriptOperator(
    dag=dag,
    task_id="autocomplete_upload",
    script="autocomplete_upload.py",
    local=True,
    json_args={
        "active_products_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
        "autocomplete_index": search.AUTOCOMPLETE_INDEX,
        "search_url": search.URL,
        "search_password": search.PASSWORD,
        "output_table": pcdefs.SEARCH_SUGGESTIONS_TABLE.get_full_name()
    },
    params={
        "active_products_table": pcdefs.ACTIVE_PRODUCTS_TABLE.get_full_name(),
        "synonyms_table": static.SYNONYMS_TABLE.get_delta_name(),
        "min_strong": 150,
        "min_include": 50
    },
    init_scripts=["install_meilisearch.sh"],
    sql="template/build_search_suggestions.sql"
)

update_trending_settings = PythonOperator(
    task_id="update_trending_settings",
    dag=dag,
    python_callable=search_settings.update_settings,
    op_kwargs={
        "synonyms_filepath": f"{DBFS_DEFS_DIR}/search/global/synonyms.json",
        "settings_filepath": f"{DBFS_DEFS_DIR}/search/trending/settings.json",
        "index_name": search.TRENDING_INDEX
    }
)

upload_trending_searches = PythonOperator(
    task_id="upload_trending_searches",
    dag=dag,
    python_callable=upload_trending_documents.add_documents,
    op_kwargs={
        "def_filepath": f"{DBFS_DEFS_DIR}/search/trending/searches.json",
        "index_name": search.TRENDING_INDEX,
        "random_order": True,
    }
)

update_label_settings = PythonOperator(
    task_id="update_label_settings",
    dag=dag,
    python_callable=search_settings.update_settings,
    op_kwargs={
        "synonyms_filepath": f"{DBFS_DEFS_DIR}/search/global/synonyms.json",
        "settings_filepath": f"{DBFS_DEFS_DIR}/search/trending/settings.json",
        "index_name": search.LABELS_INDEX
    }
)

upload_label_searches = PythonOperator(
    task_id="upload_label_searches",
    dag=dag,
    python_callable=upload_trending_documents.add_documents,
    op_kwargs={
        "def_filepath": f"{AUTOCOMPLETE_DEFS_DIR}/labels.json",
        "index_name": search.LABELS_INDEX
    }
)

trigger >> head
head >> [ update_product_search_settings, update_label_settings, 
        update_trending_settings, update_autocomplete_settings ]
head >> upload_products  >> [ upload_trending_searches, upload_label_searches ] >> tail
head >> autocomplete_upload >> tail
