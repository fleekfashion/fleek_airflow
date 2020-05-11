"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator

from src.defs.bq import personalization as pdefs, gcs_exports
from src.defs.gcs import buckets

def get_operators(dag):

    head = DummyOperator(task_id="new_product_embeddings_head", dag=dag)
    tail = DummyOperator(task_id="new_product_embeddings_tail", dag=dag)
    operators = []
    
    export_destination_table = gcs_exports.FULL_NAMES[gcs_exports.SAGEMAKER_EMBEDDER_PRODUCT_INFO]
    update_daily_sagemaker_embedder_data = BigQueryOperator(
        task_id="update_daily_sagemaker_embedder_data",
        dag=dag,
        destination_dataset_table=export_destination_table,
        write_disposition="WRITE_TRUNCATE",
        params={
            "product_info_table": pdefs.FULL_NAMES[pdefs.DAILY_NEW_PRODUCT_INFO_TABLE],
            },
        sql="template/update_sagemaker_embedder_product_info_export.sql",
        use_legacy_sql=False,
        )

    export_bucket = buckets.PREFIX + buckets.PERSONALIZATION 
    FILENAME = "sagemaker_embedder_data.csv"
    export_path = f"{export_bucket}/temp_data/aws/export/{FILENAME}"
    bq_to_gcs = BigQueryToCloudStorageOperator(
        task_id="bq_to_gcs_sagemaker_embedder_data",
        dag=dag,
        source_project_dataset_table=export_destination_table,
        destination_cloud_storage_uris=[export_path],
        export_format="CSV",
    )


    gcs_to_s3 = GoogleCloudStorageToS3Operator(
        task_id="gcs_to_s3_sagemaker_embedder_data",
        dag=dag,
        bucket=buckets.PERSONALIZATION,
        #prefix="/temp_data/aws/export/"+FILENAME,
        delimiter=".csv",
        replace=True,
        dest_s3_key="s3://fleek-prod/personalization/data/",
    )




            
    update_daily_sagemaker_embedder_data >> bq_to_gcs
    bq_to_gcs >> gcs_to_s3

    operators.append(update_daily_sagemaker_embedder_data)
    operators.append(bq_to_gcs)
    operators.append(gcs_to_s3)

    head >> operators >> tail
    return {"head":head, "tail":tail}

    
