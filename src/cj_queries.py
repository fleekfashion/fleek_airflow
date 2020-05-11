"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

import os
from datetime import timedelta

from airflow.models import DAG

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.subdags import cj_etl

DAG_ID = "daily_cj_etl_jobs"
dag = DAG(
        DAG_ID,
        default_args=DEFAULT_DAG_ARGS,
        schedule_interval=timedelta(days=1),
    )


table_setup_operators = cj_etl.table_setup.get_operators(dag)
download_operators = cj_etl.cj_download.get_operators(dag)
embeddings_operators = cj_etl.new_product_embeddings.get_operators(dag)




table_setup_operators["tail"] >> download_operators["head"] 
download_operators["tail"] >> embeddings_operators["head"] 
