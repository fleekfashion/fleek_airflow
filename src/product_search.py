"""
TLDR: Update product search meili endpoint
"""

from datetime import timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from functional import seq

from src.airflow_tools.queries import postgre_queries as pquery
from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools import dag_defs
from src.airflow_tools.databricks.databricks_operators import SparkScriptOperator, SparkSQLOperator
from src.airflow_tools.utils import get_dag_sensor
from src.callable import search_settings 
from src.defs.delta import product_catalog as pcdefs
from src.defs.delta import postgres as phooks
from src.defs.postgre import product_catalog as postdefs
from src.defs.delta.utils import DBFS_DEFS_DIR 
from src.defs import search

DAG_ID = dag_defs.PRODUCT_SEARCH
dag = DAG(
        DAG_ID,
        catchup=False,
        start_date=days_ago(1),
        schedule_interval="@daily",
        default_args=DEFAULT_DAG_ARGS,
        description=__doc__
)

trigger = get_dag_sensor(dag, dag_defs.SPARK_PRODUCT_DOWNLOAD_ETL)
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
    script="product_search_upload.py",
    local=True,
    json_args={
        "products_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
        "fields": seq(postdefs.get_columns(postdefs.PRODUCT_INFO_TABLE)) \
                .filter(lambda x: x != "is_active")
                .to_list() + ['swipe_rate'],
        "search_endpoint": search.PRODUCT_SEARCH_INDEX,
        "search_url": search.URL,
        "search_password": search.PASSWORD
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
        "active_products_table": pcdefs.get_full_name(pcdefs.ACTIVE_PRODUCTS_TABLE),
        "autocomplete_index": search.AUTOCOMPLETE_INDEX,
        "product_search_index": search.PRODUCT_SEARCH_INDEX,
        "search_url": search.URL,
        "search_password": search.PASSWORD,
        "taxonomy_path": f"{AUTOCOMPLETE_DEFS_LOCAL_DIR}/taxonomy.json",
        "global_attributes_path": f"{AUTOCOMPLETE_DEFS_LOCAL_DIR}/global_attributes.json",
        "colors_path": f"{AUTOCOMPLETE_DEFS_LOCAL_DIR}/colors.json",
        "labels_path": f"{DBFS_DEFS_DIR.replace('dbfs:/', '/dbfs/')}/product_download/global/product_labels.json",
    },
    init_scripts=["install_meilisearch.sh"]
)
trigger >> head >> update_product_search_settings >> upload_products >> tail
head >> update_autocomplete_settings >> autocomplete_upload >> tail

