"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator


from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator
from src.defs.bq import personalization as pdefs

def get_operators(dag):
    """
    Get list of all create
    table operators for cj etl
    dag
    """
    head = DummyOperator(task_id="personalization_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="personalization_table_setup_tail", dag=dag)
    operators = []

    for table_name, schema_fields in pdefs.SCHEMAS.items():
        op = BigQueryCreateTableOperator(
            task_id=f"create_{table_name}",
            dag=dag,
            project_id=pdefs.PROJECT,
            dataset_id=pdefs.DATASET,
            table_id=table_name,
            schema_fields=schema_fields, time_partitioning=pdefs.TABLE_PARTITIONS.get(table_name, None),
        )
        operators.append(op)
    head >> operators >> tail
    return {"head": head, "tail": tail}




