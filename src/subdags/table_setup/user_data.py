"""
Dag to set up
user_data tables
"""

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator


from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator
from src.defs.bq import user_data as udefs

def get_operators(dag):
    """
    Get list of all create
    table operators for cj etl
    dag
    """
    head = DummyOperator(task_id="user_data_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="user_data_table_setup_tail", dag=dag)
    operators = []

    for table_name, schema_fields in udefs.SCHEMAS.items():
        op = BigQueryCreateTableOperator(
            task_id=f"create_{table_name}",
            dag=dag,
            project_id=udefs.PROJECT,
            dataset_id=udefs.DATASET,
            table_id=table_name,
            schema_fields=schema_fields, time_partitioning=udefs.TABLE_PARTITIONS.get(table_name, None),
        )
        operators.append(op)
    head >> operators >> tail
    return {"head": head, "tail": tail}




