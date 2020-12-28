import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable, DAG

DAG_CONFIG = Variable.get("variables", deserialize_json=True)
DAG_TYPE = DAG
DEFAULT_DAG_ARGS = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': ['airflow@fleek_airflow.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'template_searchpath': ["/usr/local/airflow/dags/src/template/"],
        }

DAGS_FOLDER = os.environ.get("DAGS_FOLDER")
SRC_DIR = f"{DAGS_FOLDER}/src"
