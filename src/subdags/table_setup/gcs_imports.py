"""
DAG to delete and
recreate gcs_export
table operators
"""

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator


from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator
from src.defs.bq import gcs_imports


def get_operators(dag):
    f"""
    {__doc__} 
    """
    head = DummyOperator(task_id="table_setup_head", dag=dag)
    tail = DummyOperator(task_id="table_setup_tail", dag=dag)
    operators = []

    for table_name, schema_fields in gcs_imports.SCHEMAS.items():
        full_name = gcs_imports.FULL_NAMES[table_name]
        op1 = BigQueryTableDeleteOperator(
            task_id=f"delete_{table_name}",
            dag=dag,
            deletion_dataset_table=full_name,
            ignore_if_missing=True,
        )
        op2 = BigQueryCreateTableOperator(
            task_id=f"create_{table_name}",
            dag=dag,
            project_id=gcs_imports.PROJECT,
            dataset_id=gcs_imports.DATASET,
            table_id=table_name,
            schema_fields=schema_fields,
            time_partitioning=gcs_imports.TABLE_PARTITIONS.get(table_name, None),
        )
        op1 >> op2
        operators.extend([op1, op2])


    head >> operators >> tail
    return {"head": head, "tail": tail}




