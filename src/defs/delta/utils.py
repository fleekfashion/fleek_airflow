"""
File containing schema
definitions for delta 
tables.
"""
import os

PROJECT = os.environ.get("PROJECT", "staging")
DBFS_AIRFLOW_DIR = f"dbfs:/{PROJECT}_airflow"
DBFS_SCRIPT_DIR = f"{DBFS_AIRFLOW_DIR}/spark_scripts"

GENERAL_CLUSTER_ID = "0820-181048-frame268"
SHARED_POOL_ID = "0820-180308-fears3-pool-vNdj5Kae"
