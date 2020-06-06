from airflow.models import DAG

from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator, CloudSqlInstanceImportOperator

def get_import_operator(dag: DAG,
                        task_id: str,
                        uri: str,
                        database: str,
                        table: str,
                        instance: str,
                        filetype: str="csv",
                        columns: list=None):
    
    import_body = {
        "importContext": {
            "fileType": filetype, 
            "uri": uri,
            "database": database,
            "csvImportOptions": {
                "table": table,
            },
        },
    }

    import_body["importContext"]["csvImportOptions"]["columns"] = columns
    
    op = CloudSqlInstanceImportOperator(
        dag=dag,
        task_id=task_id,
        body=import_body,
        instance=instance,
    )
    return op


