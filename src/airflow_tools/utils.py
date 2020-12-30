from typing import Callable
from airflow.sensors.external_task_sensor import ExternalTaskSensor

def get_dag_sensor(dag,
        external_dag_id: str,
        execution_date_fn: Callable=None,
        timeout=None,
        retries=0) -> ExternalTaskSensor:
    return ExternalTaskSensor(
        dag=dag,
        task_id=f"{external_dag_id}_sensor",
        external_dag_id=external_dag_id,
        external_task_id=f"{external_dag_id}_dag_tail",
        check_existence=True,
        execution_date_fn=execution_date_fn,
        execution_timeout=timeout,
        retries=retries,
    )

def get_task_sensor(dag,
        external_dag_id: str,
        external_task_id,
        execution_date_fn: Callable=None,
        timeout=None,
        retries=0) -> ExternalTaskSensor:
    return ExternalTaskSensor(
        dag=dag,
        task_id=f"{external_task_id}_sensor",
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        check_existence=True,
        execution_date_fn=execution_date_fn,
        execution_timeout=timeout,
        retries=retries,
    )
