"""
File containing schema
definitions for delta 
tables.
"""
import os
from airflow.models import Variable

PROJECT = os.environ.get("PROJECT", "staging")
DBFS_AIRFLOW_DIR = f"dbfs:/{PROJECT}/airflow"
DBFS_DEFS_DIR = f"dbfs:/{PROJECT}/defs"
DBFS_INIT_SCRIPT_DIR = f"dbfs:/shared/init_scripts/"
DBFS_SCRIPT_DIR = f"{DBFS_AIRFLOW_DIR}/spark_scripts"
DBFS_TMP_DIR = f"{DBFS_AIRFLOW_DIR}/tmp"

SHARED_POOL_ID = "1203-171041-peso28-pool-fy1Vjl3w"

DEV_CLUSTER_ID = "1206-220043-fact841"
DEV_MODE = Variable.get("dev_mode", False)## Change to True to use dev cluster globally
