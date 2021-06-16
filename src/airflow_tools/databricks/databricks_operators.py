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
from typing import Dict, List, Union, Any, Optional

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults
from airflow.providers.databricks.operators.databricks import  _handle_databricks_operator_execution, _deep_string_coerce
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from pyspark.sql.types import StructType
from functional import seq
from src.airflow_tools.airflow_variables import SRC_DIR
from src.defs.delta.utils import DBFS_SCRIPT_DIR, SHARED_POOL_ID, DBFS_TMP_DIR, PROJECT, DBFS_INIT_SCRIPT_DIR, DEV_CLUSTER_ID, DEV_MODE

def _cp_dbfs(src: str, dest: str,
        overwrite: bool = False) -> None:
    flags = "-r --overwrite" if overwrite else "-r"
    cmd = f"dbfs cp {flags} {src} {dest}" 
    process = subprocess.run(cmd.split())
    process.check_returncode()

def _rm_dbfs(dbfs_path: str) -> None:
    cmd = f"dbfs rm  {dbfs_path}" 
    process = subprocess.run(cmd.split())

def dbfs_read_json(dbfs_path: str) -> Union[Dict[Any, Any], List[Any]]:
    json_filename = f"{random.randint(0, 2**48)}.json"
    _cp_dbfs(src=dbfs_path, dest=json_filename)

    with open(json_filename, 'r') as handle:
        data = json.load(handle)
    subprocess.run(f"rm {json_filename}".split())
    return data

class SparkScriptOperator(BaseOperator):
    template_fields = ('sql', 'json_args',)
    template_ext = ('.sql',)
    ui_color = '#f08080'
    ui_fgcolor = "white"

    @apply_defaults
    def __init__(self,
            script: str,
            json_args: dict = {},
            sql: str = None,
            cluster_id: str = None,
            params: dict = {},
            databricks_conn_id: str ='databricks_default',
            num_workers: int = None,
            min_workers: int = None,
            max_workers: int = None,
            machine_type: str = None,
            local: bool = False,
            pool_id: Optional[str] = SHARED_POOL_ID,
            spark_version: str = "8.1.x-cpu-ml-scala2.12",
            spark_conf: dict = {},
            libraries: list = [],
            init_scripts: list = [],
            polling_period_seconds=15,
            databricks_retry_limit: int=3,
            databricks_retry_delay: int=1,
            dev_mode: Optional[bool]=None,
            **kwargs
            ):

        super(SparkScriptOperator, self).__init__(**kwargs)
        self.script = script
        self.json_args: dict = { **json_args, 'params': params }
        self.sql = sql
        self.cluster_id = cluster_id
        self.local = local
        self.pool_id = pool_id
        self.params = params
        self.num_workers = num_workers
        self.min_workers= min_workers
        self.max_workers= max_workers
        self.machine_type= machine_type
        self.spark_version = spark_version
        self.spark_conf = spark_conf
        self.libraries = libraries
        self.init_scripts = init_scripts
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.run_id = None
        self.dbfs_json_path = None
        self.dev_mode = DEV_MODE and self.pool_id == SHARED_POOL_ID \
                if dev_mode is None else dev_mode 

    
    def _upload_json_to_dbfs(self, json_dict: dict):
        json_filename = f"{random.randint(0, 2**48)}.json"
        dbfs_path = f"{DBFS_TMP_DIR}/{json_filename}"
        with open(json_filename, 'w') as handle:
            json.dump(json_dict, handle)
        _cp_dbfs(json_filename, dbfs_path, overwrite=True)
        subprocess.run(f"rm {json_filename}".split())
        return dbfs_path

    def _build_spark_conf(self):
        if self.local:
            self.spark_conf.update({
                "spark.master": "local[*]"
            })

        return {"spark_conf": self.spark_conf}

    def _build_machine_type_param(self):
        params = {}
        if self.pool_id is not None:
            params.update({"instance_pool_id": self.pool_id})
        else:
            params.update({"enable_elastic_disk": True})
            params.update({"node_type_id": self.machine_type})
            params.update({"aws_attributes":{
                    'ebs_volume_count':1,
                    'ebs_volume_type': "THROUGHPUT_OPTIMIZED_HDD",
                    'ebs_volume_size':600,
                    "first_on_demand": 0
                }
            })
        return params


    def _build_spark_version_param(self):
        if self.cluster_id is None:
            return {"spark_version": self.spark_version}
        else:
            return {}
    
    def _build_init_scripts_params(self):
        def _upload_script(script: str) -> str:
            dbfs_path = f"{DBFS_INIT_SCRIPT_DIR}/{script}"
            _cp_dbfs(f"{SRC_DIR}/init_scripts/{script}", dbfs_path, overwrite=True)
            return dbfs_path
        scripts = seq(self.init_scripts) \
                .map(lambda x: x if 'dbfs:/' in x else _upload_script(x)) \
                .map(lambda x: { "dbfs" : { "destination" : x }}) \
                .to_list()
        return {
            "init_scripts": scripts
        }

    def _build_cluster_params(self):

        if self.cluster_id is not None:
            return {"existing_cluster_id": self.cluster_id}

        if self.dev_mode:
            return {"existing_cluster_id": DEV_CLUSTER_ID}

        params = {}
        if self.local:
                params.update({
                    "num_workers": 0
                    }
                )
        elif self.num_workers is not None:
            params.update({"num_workers": self.num_workers})
        else:
            params.update({
                "autoscale": {
                    "min_workers": self.min_workers,
                    "max_workers": self.max_workers
                }
            })

        params.update({"spark_version": self.spark_version})
        params.update(self._build_spark_conf())
        params.update(self._build_machine_type_param())
        params.update(self._build_init_scripts_params())
        return {"new_cluster": params}




    def _build_json_job(self):

        ## Build and upload json args
        print(self.json_args)
        args = copy.copy(self.json_args)
        args["sql"] = self.sql
        self.dbfs_json_path = self._upload_json_to_dbfs(args).replace("dbfs:/", "/dbfs/")

        ## Upload script
        dbfs_path = f"{DBFS_SCRIPT_DIR}/{self.script}"
        _cp_dbfs(f"{SRC_DIR}/spark_scripts/{self.script}", dbfs_path, overwrite=True)

        ## Build initial job
        job = {
            "run_name": f"{self.task_id}_{PROJECT}",
            "spark_python_task": {
                "python_file": dbfs_path,
                "parameters": [f"--json={self.dbfs_json_path}" ]
            },
            "libraries": [
                {"package": l } for l in self.libraries
            ]
        }

        job.update(self._build_cluster_params())
        return _deep_string_coerce(job)


    def get_hook(self):
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay)

    def execute(self, context):
        job_json = self._build_json_job()
        hook = self.get_hook()
        self.run_id = hook.submit_run(job_json)
        context['ti'].xcom_push(key="job_params", value=job_json)
        _handle_databricks_operator_execution(self, hook, self.log, context)
        _rm_dbfs(self.dbfs_json_path)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        _rm_dbfs(self.dbfs_json_path)
        self.log.info(
            f"Task: {self.task_id} with run_id: {self.run_id} was requested to be cancelled."
        )

class SparkSQLOperator(SparkScriptOperator):
    ui_color = "#97CBEC"

    def __init__(self,
        output_table: str = None,
        mode: str = None,
        output_format: str = "delta",
        drop_duplicates: bool = False,
        duplicates_subset: list = None,
        options: Dict[str, str] = {},
        **kwargs
    ):
        kwargs['script'] = "run_sql.py"
        super(SparkSQLOperator, self).__init__(**kwargs)
        self.json_args = {
            "mode": mode,
            "output_table": output_table,
            "format": output_format,
            "drop_duplicates": drop_duplicates,
            "duplicates_subset": duplicates_subset,
            "options": options,
        }


def create_table_operator(
        task_id: str,
        dag: DAG,
        table: str,
        schema: StructType,
        partition: list = None,
        comment: str = None,
        cluster_id: str = None,
        pool_id: str = SHARED_POOL_ID,
        local: bool = False,
        min_workers: int = None,
        max_workers: int = None,
        machine_type: str = None,
        polling_period_seconds: int = 30,
        dev_mode=False
        ):
    return SparkScriptOperator(
            script="create_table.py",
            json_args={
                "table": table,
                "schema": schema.jsonValue(),
                "partition": " ".join(partition) if partition else None,
                "comment": comment
            },
            task_id=task_id,
            dag=dag,
            cluster_id=cluster_id,
            pool_id=pool_id,
            local=local,
            min_workers=min_workers,
            max_workers=max_workers,
            machine_type=machine_type,
            polling_period_seconds=polling_period_seconds,
            dev_mode=dev_mode,
        )
