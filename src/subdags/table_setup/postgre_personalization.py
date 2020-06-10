"""
TLDR: Set up Postgre Personalization Tables.

"""

import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator

from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.queries import postgre_queries as pquery 
from src.defs.postgre import utils as postutils
from src.defs.postgre import personalization as postdefs

def get_operators(dag: DAG) -> dict:
    f"""
    {__doc__} 
    """
    head = DummyOperator(task_id="postgre_p_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="postgre_p_table_setup_tail", dag=dag)

    operators = []
    for table_name, table_info in postdefs.SCHEMAS.items():
        op = postgre_build_product_table = CloudSqlQueryOperator(
            dag=dag,
            gcp_cloudsql_conn_id=postdefs.CONN_ID,
            task_id=f"create_postgres_{table_name}_table",
            sql=pquery.create_table_query(
                table_name=table_name,
                columns=table_info["schema"],
                tail=table_info["tail"],
            )
        )
        operators.append(op)

    head >> operators >> tail
    return {"head": head, "tail": tail}
