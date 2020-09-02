"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

import subprocess

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator, _handle_databricks_operator_execution, _deep_string_coerce
from airflow.contrib.hooks.databricks_hook import DatabricksHook

from src.airflow_tools.airflow_variables import SRC_DIR
from src.defs.delta.utils import DBFS_SCRIPT_DIR, GENERAL_CLUSTER_ID, SHARED_POOL_ID

def _copy_to_dbfs(local_path: str, dbfs_path: str,
        overwrite: bool = False) -> None:
    flags = "-r --overwrite" if overwrite else "-r"
    cmd = f"dbfs cp {flags} {local_path} {dbfs_path}" 
    process = subprocess.run(cmd.split())

def _build_spark_job_json(
    script: str,
    parameters: list,
    cluster_id: str = None,
    pool: str = SHARED_POOL_ID,
    min_workers: int = None,
    max_workers: int = None,
    machine_type: str = None
    ):
    dbfs_path = f"{DBFS_SCRIPT_DIR}/{script}"
    _copy_to_dbfs(f"{SRC_DIR}/spark_scripts/{script}", dbfs_path, overwrite=True)
    job = {
        "spark_python_task": {
            "python_file": dbfs_path,
            "parameters": parameters
        }
    }

    if cluster_id is None:
        job["new_cluster"] = {
            "instance_pool_id": SHARED_POOL_ID,
            "autoscale": {
                "min_workers": min_workers,
                "max_workers": max_workers
            },
            "spark_version": "7.2.x-scala2.12",
            }

        if machine_type is not None:
            job["node_type_id"] = machine_type,
            job["enable_elastic_disk"] = True,
            job.pop("instance_pool_id")
    else:
        job["existing_cluster_id"] = cluster_id
    return job

def run_custom_spark_job(
    dag: DAG,
    task_id: str,
    script: str,
    parameters: list,
    cluster_id: str = None,
    pool: str = SHARED_POOL_ID,
    min_workers: int = None,
    max_workers: int = None,
    machine_type: str = None
    ):

    job = _build_spark_job_json(script=script, parameters=parameters,
            cluster_id=cluster_id, pool=pool, min_workers=min_workers,
            max_workers=max_workers, machine_type=machine_type)

    return DatabricksSubmitRunOperator(
        task_id=task_id,
        dag=dag,
        json=job,
    )


class DatabricksSQLOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
            sql: str = None,
            cluster_id: str = None,
            mode: str = None,
            output_table: str = None,
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

        super(DatabricksSQLOperator, self).__init__(**kwargs)
        self.sql = sql
        self.cluster_id= cluster_id
        self.mode = mode
        self.params = params
        self.output_table = output_table
        self.min_workers= min_workers
        self.max_workers= max_workers
        self.machine_type= machine_type

        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.run_id = None
    
    def _build_json(self):
        ## Build parameters list
        parameters = [f"--sql={self.sql}" ]
        if self.mode is not None:
            parameters.append(f"--mode={self.mode}")
        if self.output_table is not None:
            parameters.append(f"--output_table={self.output_table}")

        json = _build_spark_job_json(
            script="run_sql.py",
            parameters=parameters,
            cluster_id=self.cluster_id,
            min_workers=self.min_workers,
            max_workers=self.max_workers,
            machine_type=self.machine_type
        )
        return _deep_string_coerce(json)

    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def execute(self, context):
        hook = self.get_hook()
        self.run_id = hook.submit_run(self._build_json())
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        self.log.info(
            f"Task: {self.task_id} with run_id: {self.run_id} was requested to be cancelled."
        )

