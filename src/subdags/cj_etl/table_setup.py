
"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

from datetime import timedelta

from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools.operators.bq_create_table_operator import BigQueryCreateTableOperator

from src.defs.bq import personalization as pdefs
from src.defs.bq import gcs_exports
from src.defs.bq.datasets import PERSONALIZATION, GCS_EXPORTS

DAILY_DELETE = [pdefs.DAILY_NEW_PRODUCT_INFO_TABLE ]

def get_operators(dag):
    """
    Get list of all create
    table operators for cj etl
    dag
    """

    operators = []
    for table_name, schema_fields in pdefs.SCHEMAS.items():

        op = BigQueryCreateTableOperator(
            task_id=f"create_{table_name}",
            dag=dag,
            project_id=pdefs.PROJECT,
            dataset_id=PERSONALIZATION,
            table_id=table_name,
            schema_fields=schema_fields,
            time_partitioning=pdefs.TABLE_PARTITIONS.get(table_name, None),
        )

        if table_name in DAILY_DELETE:
            full_name = pdefs.FULL_NAMES[table_name]
            op1 = BigQueryTableDeleteOperator(
                task_id=f"delete_{table_name}",
                dag=dag,
                deletion_dataset_table=full_name,
            )
            op1 >> op
            operators.append(op1)
        operators.append(op)

    for table_name, schema_fields in gcs_exports.SCHEMAS.items():

        full_name = gcs_exports.FULL_NAMES[table_name]
        op1 = BigQueryTableDeleteOperator(
            task_id=f"delete_{table_name}",
            dag=dag,
            deletion_dataset_table=full_name,
        )

        op2 = BigQueryCreateTableOperator(
            task_id=f"create_{table_name}",
            dag=dag,
            project_id=gcs_exports.PROJECT,
            dataset_id=GCS_EXPORTS,
            table_id=pdefs.DAILY_CJ_DOWNLOAD_TABLE,
            schema_fields=schema_fields,
            time_partitioning=gcs_exports.TABLE_PARTITIONS.get(table_name, None),
        )

        op1 >> op2
        operators.extend([op1, op2])

    return operators




