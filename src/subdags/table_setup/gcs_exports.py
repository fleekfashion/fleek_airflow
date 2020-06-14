"""
DAG to delete and
recreate gcs_export
table operators
"""

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator


from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator
from src.defs.bq import gcs_exports 


def get_operators(dag):
    f"""
    {__doc__}
    """
    head = DummyOperator(task_id="gcs_exports_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="gcs_exports_table_setup_tail", dag=dag)
    operators = []

    for table_name, schema_fields in gcs_exports.SCHEMAS.items():
        full_name = gcs_exports.FULL_NAMES[table_name]
        op = BigQueryCreateTableOperator(
            task_id=f"create_{table_name}",
            dag=dag,
            project_id=gcs_exports.PROJECT,
            dataset_id=gcs_exports.DATASET,
            table_id=table_name,
            schema_fields=schema_fields,
            time_partitioning=gcs_exports.TABLE_PARTITIONS.get(table_name, None),
        )
        operators.append(op)

    head >> operators >> tail
    return {"head": head, "tail": tail}





