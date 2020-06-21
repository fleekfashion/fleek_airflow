from airflow.models import DAG
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator, CloudSqlInstanceImportOperator, CloudSqlInstanceExportOperator

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

def get_export_operator(dag: DAG,
                        task_id: str,
                        uri: str,
                        database: str,
                        instance: str,
                        query: str,
                        filetype: str = "CSV"):
    export_body = {
        "exportContext": {
            "csvExportOptions": {
                "selectQuery": query
            },
            "databases": [database],
            "fileType": filetype,
            "uri": uri
        }
    }

    op = CloudSqlInstanceExportOperator(
        dag=dag,
        task_id=task_id,
        body=export_body,
        instance=instance,
    )
    return op
