from airflow.sensors.external_task_sensor import ExternalTaskSensor
from src import table_setup

def get_table_setup_sensor(dag):
    sensor = ExternalTaskSensor(
        dag=dag,
        task_id="table_setup_sensor",
        external_dag_id=table_setup.DAG_ID,
        external_task_id=f"{table_setup.DAG_ID}_dag_tail"
    )
    return sensor
    
