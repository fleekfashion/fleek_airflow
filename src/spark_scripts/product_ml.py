import io
import subprocess
import argparse
import json

import tensorflow as tf
import pandas as pd
import numpy as np
import cv2

from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql import functions as F
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except:
    pass


parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")

IMG_DIR = json_args["img_dir"] 
MODEL_PATH = json_args["model_path"]
VERSION = json_args["version"] 
DEST_TABLE = json_args["dest_table"]

images = spark.read.format("binaryFile") \
 .option("pathGlobFilter", "*.jpg") \
 .load(IMG_DIR)

res = subprocess.run(f"cp -R {MODEL_PATH}/{VERSION} ./".split())
m = tf.keras.models.load_model(VERSION)

bc_model_json = sc.broadcast(m.to_json())
bc_model_weights = sc.broadcast(m.get_weights())

def model_fn():
    """
    Returns a ResNet50 model with top layer removed and broadcasted pretrained weights.
    """
    model = tf.keras.models.load_model(bc_model_path.value)
    return model

def model_fn():
    """
    Returns a ResNet50 model with top layer removed and broadcasted pretrained weights.
    """
    model = tf.keras.models.model_from_json(bc_model_json.value)
    model.set_weights(bc_model_weights.value)
    return model

def preprocess(content):
    """
    Preprocesses raw image bytes for prediction.
    """
    b = io.BytesIO(content)
    file_bytes = np.asarray(bytearray(b.read()), dtype=np.uint8)
    img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR, )[:, :, ::-1]
    arr = cv2.resize(img, (200, 200)).astype("float32")
    arr /= 255
    arr -= .5
    arr *= 2
    return arr

def _normalize_matrix_rows(m: np.array) -> np.array:
    magnitude = np.sum(np.square(m), axis=1)
    magnitude = np.sqrt(magnitude).reshape([len(m), 1])
    return m/magnitude

def featurize_series(model, content_series):
    """
    Featurize a pd.Series of raw images using the input model.
    :return: a pd.Series of image features
    """
    input = np.stack(content_series.map(preprocess))
    preds = model.predict(input)
    preds = _normalize_matrix_rows(preds)

    # For some layers, output features will be multi-dimensional tensors.
    # We flatten the feature tensors to vectors for easier storage in Spark DataFrames.
    output = [p.flatten() for p in preds]
    return pd.Series(output)

@pandas_udf('array<float>', PandasUDFType.SCALAR_ITER)
def featurize_udf(content_series_iter):
    model = model_fn()
    for content_series in content_series_iter:
        yield featurize_series(model, content_series)

def get_pid():
    x = F.reverse(F.split(F.col("path"), "/"))[0]
    x = F.split(x, "\.")[0].cast("bigint").alias("product_id")
    return x

features_df = images.repartition(1) \
    .withColumn("product_image_embedding", featurize_udf("content")) \
    .withColumn("product_id", get_pid()) \
    .select(["product_image_embedding", "product_id"])

features_df.write.saveAsTable(DEST_TABLE, format="delta", mode="overwrite")
