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
from src.airflow_tools.utils import get_task_sensor
from src.airflow_tools.dag_defs import USER_EVENTS
from src.airflow_tools.operators.bq_safe_truncate_operator import get_safe_truncate_operator
from src.callable.push_docker_image import build_repo_uri
from src.callable.sagemaker_processing import run_processing
from src.callable.sagemaker_transform import run_transform

# Global Args 
S3_BASE_DIR = f"s3://{buckets.MAIN}/personalization/temp/sagemaker/recommenders/v1"
MACHINE_OUTPUT_SOURCE = "/opt/ml/processing/output"
BASE_INPUT_DEST = "/opt/ml/processing/input"
ECR_REPO = DAG_CONFIG["ecr_images"]["product_recommender"]
BQ_OUTPUT_TABLE = f"{gcs_imports.PROJECT}.{gcs_imports.DATASET}.user_product_recommendations"
TOP_N = DAG_CONFIG.get("model_parameters").get(
    "product_recommender").get(
    "top_n")

def get_operators(dag: DAG_TYPE) -> dict:
    f"{__doc__}"

    head = DummyOperator(task_id="product_recs_head", dag=dag)
    tail = DummyOperator(task_id="product_recs_tail", dag=dag)

    user_data_update_sensor = get_task_sensor(
        dag=dag,
        external_dag_id=USER_EVENTS,
        external_task_id="append_user_events"
    )

    truncate_user_events_agg = get_safe_truncate_operator(
        dag,
        pdefs.get_full_name(pdefs.AGGREGATED_USER_DATA_TABLE)
    )

    user_events_aggregation = BigQueryOperator(
        sql="template/user_events_aggregation.sql",
        dag=dag,
        task_id="build_user_rec_events",
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
        destination_dataset_table=pdefs.get_full_name(
            pdefs.AGGREGATED_USER_DATA_TABLE
            )
    )

    delete_rec_table = BigQueryTableDeleteOperator(
        task_id=f"delete_bq_sagemaker_import_rec_table",
        dag=dag,
        deletion_dataset_table=BQ_OUTPUT_TABLE,
        ignore_if_missing=True
    )

    arguments = [
        f"--project={gcs_imports.PROJECT}",
        f"--top_n={TOP_N}",
        f"--bq_output_table={BQ_OUTPUT_TABLE}",
    ]
    recommender_kwargs = {
        "docker_img_uri": build_repo_uri(ECR_REPO),
        "processing_filepath": f"{SRC_DIR}/sagemaker_scripts/product_recommender/build_and_run_model/build_and_run_model.py",
        "arguments": arguments
    }

    product_recommender = PythonOperator(
        task_id="sagemaker_product_recommender",
        dag=dag,
        python_callable=run_processing,
        op_kwargs=recommender_kwargs,
        provide_context=False
    )

    head >> user_data_update_sensor >> truncate_user_events_agg
    truncate_user_events_agg >> user_events_aggregation >> delete_rec_table
    delete_rec_table >> product_recommender >> tail

    return {"head":head, "tail":tail}
