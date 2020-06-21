from airflow.sensors.external_task_sensor import ExternalTaskSensor
from src import table_setup

def get_dag_sensor(dag, external_dag_id):
    sensor = ExternalTaskSensor(
        dag=dag,
        task_id=f"{external_dag_id}_sensor",
        external_dag_id=external_dag_id,
        external_task_id=f"{external_dag_id}_dag_tail",
        check_existence=True
    )
    return sensor

def get_task_sensor(dag, external_dag_id, external_task_id):
    sensor = ExternalTaskSensor(
        dag=dag,
        task_id=f"{external_task_id}_sensor",
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        check_existence=True
    )
    return sensor
