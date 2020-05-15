"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator

from src.defs.bq import personalization as pdefs, gcs_exports
from src.defs.gcs import buckets
from src.airflow_tools.airflow_variables import SRC_DIR 
from src.callable.push_docker_image import build_repo_uri
from src.callable.sagemaker_processing import run_processing 
from src.callable.sagemaker_transform import run_transform
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

    export_bucket = buckets.PREFIX + buckets.EXPORTS
    export_path = f"personalization/temp/sagemaker/embedding_models/input/embedder_data.csv"
    uri =f"{export_bucket}/{export_path}"

    bq_to_gcs = BigQueryToCloudStorageOperator(
        task_id="bq_to_gcs_sagemaker_embedder_data",
        dag=dag,
        source_project_dataset_table=export_destination_table,
        destination_cloud_storage_uris=[uri],
        export_format="CSV",
    )

    S3_BUCKET = buckets.MAIN
    S3_PATH = export_path
    FULL_S3_PATH = "s3://{buckets.MAIN}/{S3_PATH}"
    gcs_to_s3 = GoogleCloudStorageToS3Operator(
        task_id="gcs_to_s3_sagemaker_embedder_data",
        dag=dag,
        bucket=buckets.EXPORTS,
        prefix=export_path,
        replace=True,
        dest_s3_key=f"s3://{buckets.MAIN}/"
    )
   
    ECR_REPO = "testpreproc"
    img_uri = build_repo_uri(ECR_REPO)

    S3_DIR = f"s3://{buckets.MAIN}/personalization/temp/sagemaker/embedding_models/input/"
    IMG_FILENAME = "images.jsonl"
    PID_FILENAME = "pids.jsonl"

    preproc_kwargs = {
            "docker_img_uri": img_uri,
            "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/inception_embeddings/PreProcessing/preprocessing.py",
            "s3_output_dir": S3_DIR,
            "output_filename": IMG_FILENAME,
            "pid_output_filename": PID_FILENAME
            }
    proc_data = PythonOperator(
        task_id="run_proc2",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=preproc_kwargs,
        provide_context=False
    )
    
    ts = int(datetime.datetime.now().timestamp())
    job_name = f"embeddings{ts}"
    transform_output_path = "s3://fleek-prod/personalization/temp/sagemaker/embedding_models/output"

    transform_kwargs = {
            "model_data": 's3://fleek-prod/personalization/models/embedding_models/model4.tar.gz',
            "input_data": "s3://fleek-prod/personalization/temp/sagemaker/embedding_models/input/images.jsonl",
            "output_path": transform_output_path,
            "job_name": job_name,
            "instance_type": "ml.p2.xlarge",
            "max_payload": 50,
    }

    embedding_transform = PythonOperator(
        task_id="embedding_transform",
        dag=dag,
        python_callable=run_transform,
        op_kwargs=transform_kwargs,
        provide_context=False
    )
            
    update_daily_sagemaker_embedder_data >> bq_to_gcs
    bq_to_gcs >> gcs_to_s3 >> proc_data >> embedding_transform

    operators.append(update_daily_sagemaker_embedder_data)
    operators.append(bq_to_gcs)
    operators.append(gcs_to_s3)
    operators.append(proc_data)
    operators.append(embedding_transform)

    head >> operators >> tail
    return {"head":head, "tail":tail}

    
