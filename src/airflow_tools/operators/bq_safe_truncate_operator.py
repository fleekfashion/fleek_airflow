from airflow.contrib.operators.bigquery_operator import BigQueryOperator

def get_safe_truncate_operator(dag, table):
    task_id = f"safe_truncate_{table}"
    op = BigQueryOperator(
        dag=dag,
        task_id=task_id,
        params={"table":table},
        sql="DELETE FROM {{params.table}} WHERE 1=1",
        use_legacy_sql=False,
    )
    return op
