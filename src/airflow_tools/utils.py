from airflow.sensors.external_task_sensor import ExternalTaskSensor
from pendulum import Pendulum

def nearest_day(execution_date: Pendulum) -> Pendulum:
    return execution_date.date()

def nearest_hour(execution_date: Pendulum) -> Pendulum:
    return execution_date.hour_(execution_date.hour)

def get_dag_sensor(dag, external_dag_id,
        execution_date_fn=None):
    sensor = ExternalTaskSensor(
        dag=dag,
        task_id=f"{external_dag_id}_sensor",
        external_dag_id=external_dag_id,
        external_task_id=f"{external_dag_id}_dag_tail",
        check_existence=True,
        execution_date_fn=execution_date_fn,
    )
    return sensor

def get_task_sensor(dag, external_dag_id, external_task_id,
        execution_date_fn=None):
    sensor = ExternalTaskSensor(
        dag=dag,
        task_id=f"{external_task_id}_sensor",
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        check_existence=True,
        execution_date_fn=execution_date_fn
    )
    return sensor

