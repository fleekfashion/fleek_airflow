"""
DAG to run queries to CJ
and download the data to a
daily BQ table.
"""

import os

from src.defs.bq import personalization as pdefs
from src.defs.bq import gcs_exports as g_exports
from src.defs.bq.datasets import PERSONALIZATION as DATASET, GCS_EXPORTS

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")
FULL_CJ_DOWNLOAD_TABLE = ".".join([PROJECT, DATASET, pdefs.DAILY_CJ_DOWNLOAD_TABLE])
FULL_HISTORIC_PRODUCTS_TABLE = ".".join([PROJECT, DATASET, pdefs.HISTORIC_PRODUCTS_TABLE])
FULL_ACTIVE_PRODUCTS_TABLE = ".".join([PROJECT, DATASET, pdefs.ACTIVE_PRODUCTS_TABLE])
FULL_DAILY_NEW_PRODUCT_INFO_TABLE = ".".join([PROJECT, DATASET, pdefs.DAILY_NEW_PRODUCT_INFO_TABLE])
FULL_SAGEMAKER_EMBEDDER_PRODUCT_INFO = ".".join(
        [
            PROJECT,
            GCS_EXPORTS,
            g_exports.SAGEMAKER_EMBEDDER_PRODUCT_INFO
        ]
    )
