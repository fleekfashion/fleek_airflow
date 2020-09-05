"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

import subprocess
import random
import json
import copy

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator, _handle_databricks_operator_execution, _deep_string_coerce
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from pyspark.sql.types import StructType

from src.airflow_tools.airflow_variables import SRC_DIR
from src.defs.delta.utils import DBFS_SCRIPT_DIR, GENERAL_CLUSTER_ID, SHARED_POOL_ID, DBFS_TMP_DIR

def _copy_to_dbfs(local_path: str, dbfs_path: str,
        overwrite: bool = False) -> None:
    flags = "-r --overwrite" if overwrite else "-r"
    cmd = f"dbfs cp {flags} {local_path} {dbfs_path}" 
    process = subprocess.run(cmd.split())

def _rm_dbfs(dbfs_path: str) -> None:
    cmd = f"dbfs rm  {dbfs_path}" 
    process = subprocess.run(cmd.split())


class SparkScriptOperator(BaseOperator):
    template_fields = ('sql', 'json_args',)
    template_ext = ('.sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
            script: str = None,
            json_args: dict = {},
            sql: str = None,
            cluster_id: str = None,
            pool_id: str = SHARED_POOL_ID,
            params: dict = {},
            databricks_conn_id: str ='databricks_default',
            min_workers: int = None,
            max_workers: int = None,
            machine_type: str = None,
            polling_period_seconds=30,
            databricks_retry_limit: int=3,
            databricks_retry_delay: int=1,
            **kwargs
            ):

        super(SparkScriptOperator, self).__init__(**kwargs)
        self.script = script
        self.json_args = json_args
        self.sql = sql
        self.cluster_id= cluster_id
        self.pool_id = pool_id 
        self.params = params
        self.min_workers= min_workers
        self.max_workers= max_workers
        self.machine_type= machine_type

        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.run_id = None
        self.dbfs_json_path = None
    
    def _upload_json_to_dbfs(self, json_dict: dict):
        json_filename = f"{random.randint(0, 2**48)}.json"
        dbfs_path = f"{DBFS_TMP_DIR}/{json_filename}"
        with open(json_filename, 'w') as handle:
            json.dump(json_dict, handle)
        _copy_to_dbfs(json_filename, dbfs_path, overwrite=True)
        subprocess.run(f"rm {json_filename}".split())
        return dbfs_path

    def _build_json_job(self):

        ## Build and upload json args
        args = copy.copy(self.json_args)
        args["sql"] = self.sql
        self.dbfs_json_path = self._upload_json_to_dbfs(args).replace("dbfs:/", "/dbfs/")

        ## Upload script
        dbfs_path = f"{DBFS_SCRIPT_DIR}/{self.script}"
        _copy_to_dbfs(f"{SRC_DIR}/spark_scripts/{self.script}", dbfs_path, overwrite=True)

        ## Build initial job
        job = {
            "spark_python_task": {
                "python_file": dbfs_path,
                "parameters": [f"--json={self.dbfs_json_path}" ]
            }
        }

        if self.cluster_id is None:
            job["new_cluster"] = {
                "instance_pool_id": SHARED_POOL_ID,
                "autoscale": {
                    "min_workers": self.min_workers,
                    "max_workers": self.max_workers
                },
                "spark_version": "7.2.x-scala2.12",
                }

            if self.machine_type is not None:
                job["node_type_id"] = self.machine_type,
                job["enable_elastic_disk"] = True,
                job.pop("instance_pool_id")
        else:
            job["existing_cluster_id"] = self.cluster_id
        return _deep_string_coerce(job)


    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def execute(self, context):
        job_json = self._build_json_job()
        print(self.sql)
        print(job_json)

        hook = self.get_hook()
        self.run_id = hook.submit_run(job_json)
        _handle_databricks_operator_execution(self, hook, self.log, context)
        _rm_dbfs(self.dbfs_json_path)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        _rm_dbfs(self.dbfs_json_path)
        self.log.info(
            f"Task: {self.task_id} with run_id: {self.run_id} was requested to be cancelled."
        )

def spark_sql_operator(
        sql: str,
        task_id: str,
        dag: DAG,
        params: dict = {},
        cluster_id: str = None,
        pool_id: str = SHARED_POOL_ID,
        min_workers: int = None,
        max_workers: int = None,
        machine_type: str = None,
        polling_period_seconds: int = 30,
        ):
    return SparkScriptOperator(
            script="run_sql.py",
            sql=sql,
            task_id=task_id,
            dag=dag,
            params=params,
            cluster_id=cluster_id,
            pool_id=pool_id,
            min_workers=min_workers,
            max_workers=max_workers,
            machine_type=machine_type,
            polling_period_seconds=polling_period_seconds,
        )

def create_table_operator(
        task_id: str,
        dag: DAG,
        table: str,
        schema: StructType,
        partition: list = None,
        comment: str = None,
        cluster_id: str = None,
        pool_id: str = SHARED_POOL_ID,
        min_workers: int = None,
        max_workers: int = None,
        machine_type: str = None,
        polling_period_seconds: int = 30,
        ):
    return SparkScriptOperator(
            script="create_table.py",
            json_args={
                "table": table,
                "schema": schema.jsonValue(),
                "partition": partition,
                "comment": comment
            },
            task_id=task_id,
            dag=dag,
            cluster_id=cluster_id,
            pool_id=pool_id,
            min_workers=min_workers,
            max_workers=max_workers,
            machine_type=machine_type,
            polling_period_seconds=polling_period_seconds,
        )
