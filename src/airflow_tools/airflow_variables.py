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
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'template_searchpath': ["/usr/local/airflow/dags/src/template/"],
        }

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
SRC_DIR = f"{AIRFLOW_HOME}/dags/src"
