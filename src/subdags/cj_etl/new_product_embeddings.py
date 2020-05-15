"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

import datetime
import os

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput

from src.defs.bq import personalization as pdefs, gcs_exports, gcs_imports
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


    
    ## Preprocess data for embedding
    ## transformation
    #
    ECR_REPO = "testpreproc"
    img_uri = build_repo_uri(ECR_REPO)

    S3_BASE_DIR = f"s3://{buckets.MAIN}/personalization/temp/sagemaker/embedding_models"
    DEST_DIR = f"{S3_BASE_DIR}/input"
    IMG_FILENAME = "images.jsonl"
    PID_FILENAME = "pids.jsonl"
    output_source = '/opt/ml/processing/output/'
    outputs = [ProcessingOutput(destination=DEST_DIR,
                                source=output_source)]

    arguments = [
            f"--pid_out={output_source}/{PID_FILENAME}",
            f"--main_out={output_source}/{IMG_FILENAME}"
        ]

    preproc_kwargs = {
            "docker_img_uri": img_uri,
            "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/inception_embeddings/PreProcessing/preprocessing.py",
            "outputs": outputs,
            "arguments": arguments
            }

    proc_data = PythonOperator(
        task_id="run_preprocessing",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=preproc_kwargs,
        provide_context=False
    )
    

    ## Get embeddings batch transform
    #k
    ts = int(datetime.datetime.now().timestamp())
    job_name = f"embeddings{ts}"
    transform_output_path = "s3://fleek-prod/personalization/temp/sagemaker/embedding_models/output"

    transform_kwargs = {
            "model_data": 's3://fleek-prod/personalization/models/embedding_models/model4.tar.gz',
            "input_data": f"{DEST_DIR}/{IMG_FILENAME}",
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

    EMB_PATH = f"{transform_output_path}/{IMG_FILENAME}.out"
    PID_PATH = f"{DEST_DIR}/{PID_FILENAME}"
    BQ_OUT_TABLE = f"{gcs_imports.PROJECT}.{gcs_imports.DATASET}.{gcs_imports.DAILY_NEW_PRODUCT_EMBEDDINGS_TABLE}"
    
    INPUT_DEST1 = "/opt/ml/processing/input1"
    INPUT_DEST2 = "/opt/ml/processing/input2"
    inputs = [
           ProcessingInput(
               source=EMB_PATH,
               destination=f"{INPUT_DEST1}",
               input_name=f"{IMG_FILENAME}.out"
            ),
           ProcessingInput(
               source=PID_PATH,
               destination=f"{INPUT_DEST2}",
               input_name=f"{PID_FILENAME}"
           )
       ]
    
    arguments = [
            f"--input_path={INPUT_DEST1}/{IMG_FILENAME}.out",
            f"--pid_input_path={INPUT_DEST2}/{PID_FILENAME}",
            f"--bq_output_table={BQ_OUT_TABLE}"
        ]
    
    img_uri = build_repo_uri(ecr_repo="embedding-postprocessing")
    postproc_kwargs = {
            "docker_img_uri": img_uri,
            "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/inception_embeddings/PostProcessing/processing.py",
            "inputs": inputs,
            "arguments": arguments
            }

    postproc = PythonOperator(
        task_id="post_processing",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=postproc_kwargs,
        provide_context=False
    )
            
    update_daily_sagemaker_embedder_data 
    proc_data >> embedding_transform >> postproc

    operators.append(update_daily_sagemaker_embedder_data)
    operators.append(proc_data)
    operators.append(embedding_transform)
    operators.append(postproc)

    head >> operators >> tail
    return {"head":head, "tail":tail}

    
