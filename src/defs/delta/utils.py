"""
File containing schema
definitions for delta 
tables.
"""
import os

PROJECT = os.environ.get("PROJECT", "staging")
DBFS_AIRFLOW_DIR = f"dbfs:/{PROJECT}/airflow"
DBFS_DEFS_DIR = f"dbfs:/{PROJECT}/defs"
DBFS_SCRIPT_DIR = f"{DBFS_AIRFLOW_DIR}/spark_scripts"
DBFS_TMP_DIR = f"{DBFS_AIRFLOW_DIR}/tmp"

GENERAL_CLUSTER_ID = "0919-005001-newts346"
SHARED_POOL_ID = "0919-004812-sumac18-pool-T1EM79A2"
