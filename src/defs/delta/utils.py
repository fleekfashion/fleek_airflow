"""
File containing schema
definitions for delta 
tables.
"""
import os

PROJECT = os.environ.get("PROJECT", "staging")
DBFS_AIRFLOW_DIR = f"dbfs:/{PROJECT}_airflow"
DBFS_SCRIPT_DIR = f"{DBFS_AIRFLOW_DIR}/spark_scripts"
DBFS_TMP_DIR = f"{DBFS_AIRFLOW_DIR}/tmp"

GENERAL_CLUSTER_ID = "0906-213043-havoc263"
SHARED_POOL_ID = "0906-212737-viced43-pool-P14LNi6H"
