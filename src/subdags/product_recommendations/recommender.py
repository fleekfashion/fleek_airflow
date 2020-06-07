"""
DAG to run product
recommendations
"""

import datetime

from google.cloud import bigquery as bq

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from sagemaker.processing import ProcessingInput, ProcessingOutput

from src.defs.bq import personalization as pdefs, gcs_exports, gcs_imports
from src.defs.gcs import buckets
from src.airflow_tools.airflow_variables import SRC_DIR, DAG_CONFIG, DAG_TYPE
from src.callable.push_docker_image import build_repo_uri
from src.callable.sagemaker_processing import run_processing
from src.callable.sagemaker_transform import run_transform

# Global Args 
S3_BASE_DIR = f"s3://{buckets.MAIN}/personalization/temp/sagemaker/recommenders/v1"
MACHINE_OUTPUT_SOURCE = "/opt/ml/processing/output"
ECR_REPO = DAG_CONFIG["ecr_images"]["product_recommender"]

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"

    head = DummyOperator(task_id="product_recs_head", dag=dag)
    tail = DummyOperator(task_id="product_recs_tail", dag=dag)

    ## Build Model VARS
    MODEL_FILENAME = "model.tar.gz"
    PROCESSOR_FILENAME = "processor.pickle"
    TOP_N = DAG_CONFIG.get("model_parameters").get(
        "product_recommender").get(
        "top_n")

    outputs = [ProcessingOutput(destination=S3_BASE_DIR,
                                source=MACHINE_OUTPUT_SOURCE)]
    arguments = [
        f"--processor_out={MACHINE_OUTPUT_SOURCE}/{PROCESSOR_FILENAME}",
        f"--model_out={MACHINE_OUTPUT_SOURCE}/{MODEL_FILENAME}",
        f"--project={pdefs.PROJECT}",
        f"--top_n={TOP_N}"
    ]

    build_model_kwargs = {
        "docker_img_uri": build_repo_uri(ECR_REPO),
        "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/product_recommender/build_model/build_model.py",
        "outputs": outputs,
        "arguments": arguments
    }

    build_model = PythonOperator(
        task_id="build_model",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=build_model_kwargs,
        provide_context=False
    )


    PREPROC_DEST_DIR = f"{S3_BASE_DIR}/input"
    ID_FILENAME = "user_ids.jsonl"
    USER_DATA_FILENAME = "user_product_data.jsonl"
    outputs = [ProcessingOutput(destination=PREPROC_DEST_DIR,
                                source=MACHINE_OUTPUT_SOURCE)]
    arguments = [
        f"--uid_out={MACHINE_OUTPUT_SOURCE}/{ID_FILENAME}",
        f"--main_out={MACHINE_OUTPUT_SOURCE}/{USER_DATA_FILENAME}",
        f"--project={pdefs.PROJECT}"
    ]
    
    preprocessing_kwargs = {
        "docker_img_uri": build_repo_uri(ECR_REPO),
        "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/product_recommender/preprocessing/processing.py",
        "outputs": outputs,
        "arguments": arguments
    }

    preprocessing = PythonOperator(
        task_id="sagemaker_recommender_preprocessing",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=preprocessing_kwargs,
        provide_context=False
    )


    ## TRANSFORM
    ## Run NN to get embeddings
    ts = int(datetime.datetime.now().timestamp())
    job_name = f"product-recommendations-{ts}"
    TRANSFORM_OUTPUT_PATH = f"{S3_BASE_DIR}/output"
    transform_kwargs = {
            "model_data": DAG_CONFIG["model_uris"]["product_recommender"],
            "input_data": f"{PREPROC_DEST_DIR}/{USER_DATA_FILENAME}",
            "output_path": TRANSFORM_OUTPUT_PATH,
            "job_name": job_name,
            "instance_type": "ml.m5.xlarge",#"ml.p2.xlarge",
            "max_payload": 50,
    }
    recommender_transform = PythonOperator(
        task_id="recommender_batch_transform",
        dag=dag,
        python_callable=run_transform,
        op_kwargs=transform_kwargs,
        provide_context=False
    )
    
    ## POSTPROC
    RECOMMENDATIONS_FILENAME = USER_DATA_FILENAME + ".out"

    BASE_INPUT_DEST = "/opt/ml/processing/input"
    REC_INPUT_DEST = f"{BASE_INPUT_DEST}/rec"
    ID_INPUT_DEST = f"{BASE_INPUT_DEST}/id"
    PROC_INPUT_DEST = f"{BASE_INPUT_DEST}/proc"
    BQ_OUTPUT_TABLE = f"{pdefs.PROJECT}.{gcs_imports.DATASET}.user_product_recommendations"

    inputs = [
           ProcessingInput(
               source=f"{TRANSFORM_OUTPUT_PATH}/{RECOMMENDATIONS_FILENAME}",
               destination=f"{REC_INPUT_DEST}",
               input_name=f"{RECOMMENDATIONS_FILENAME}"
            ),
           ProcessingInput(
               source=f"{PREPROC_DEST_DIR}/{ID_FILENAME}",
               destination=f"{ID_INPUT_DEST}",
               input_name=f"{ID_FILENAME}"
            ),
           ProcessingInput(
               source=f"{S3_BASE_DIR}/{PROCESSOR_FILENAME}",
               destination=f"{PROC_INPUT_DEST}",
               input_name=f"{PROCESSOR_FILENAME}"
            ),
       ]


    arguments = [
        f"--main_input_path={REC_INPUT_DEST}/{RECOMMENDATIONS_FILENAME}",
        f"--id_input_path={ID_INPUT_DEST}/{ID_FILENAME}",
        f"--processor_path={PROC_INPUT_DEST}/{PROCESSOR_FILENAME}",
        f"--project={pdefs.PROJECT}",
        f"--bq_output_table={BQ_OUTPUT_TABLE}",
    ]
    
    postprocessing_kwargs = {
        "docker_img_uri": build_repo_uri(ECR_REPO),
        "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/product_recommender/postprocessing/processing.py",
        "inputs": inputs,
        "arguments": arguments
    }
    
    delete_rec_table = BigQueryTableDeleteOperator(
        task_id=f"delete_bq_sagemaker_import_rec_table",
        dag=dag,
        deletion_dataset_table=BQ_OUTPUT_TABLE,
        ignore_if_missing=True
    )

    postprocessing = PythonOperator(
        task_id="sagemaker_recommender_postprocessing",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=postprocessing_kwargs,
        provide_context=False
    )

    
    head >> build_model >> preprocessing
    preprocessing >> recommender_transform >> delete_rec_table
    delete_rec_table >> postprocessing >> tail

    return {"head":head, "tail":tail}
