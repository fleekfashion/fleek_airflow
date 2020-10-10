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
from src.airflow_tools.databricks.databricks_operators import spark_sql_operator
from src.defs.postgre import utils as postutils
from src.defs import postgre
from src.defs.delta import postgres as spark_postgre

POSTDEFS = [postgre.product_catalog, postgre.user_data, postgre.spark_personalization]

def get_operators(dag: DAG) -> dict:
    f"""
    {__doc__} 
    """
    head = DummyOperator(task_id="postgre_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="postgre_table_setup_tail", dag=dag)

    for postdefs in POSTDEFS:
        for orig_table_name, table_info in postdefs.SCHEMAS.items():
            for prefix in ["", "staging_"]:
                table_name = prefix + orig_table_name

                op1 = postgre_build_product_table = CloudSqlQueryOperator(
                    dag=dag,
                    gcp_cloudsql_conn_id=postdefs.CONN_ID,
                    task_id=f"create_postgres_{table_name}_table",
                    sql=pquery.create_table_query(
                        table_name=postdefs.get_full_name(table_name),
                        columns=table_info["schema"],
                        tail=table_info["tail"].replace(orig_table_name, table_name),
                        is_prod=len(prefix) == 0,
                    )
                )

                op2 = spark_sql_operator(
                    task_id=f"create_postgres_{table_name}_delta_hook",
                    dag=dag,
                    params={
                        "table": spark_postgre.get_full_name(table_name),
                        "url": os.environ["SPARK_CLOUD_SQL_URL"],
                        "dbtable": f"{postdefs.get_full_name(table_name)}",
                        "user": os.environ["CLOUD_SQL_USER"],
                        "password": os.environ["CLOUD_SQL_PASSWORD"]
                    },
                    sql="template/jdbc_delta_hook.sql",
                    local=True
                )
                head >> op1 >> op2 >> tail

    return {"head": head, "tail": tail}
