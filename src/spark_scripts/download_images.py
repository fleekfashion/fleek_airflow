import typing as t
import urllib.request
import shutil# Download URL and save to outpath.
import os
import pathlib
import argparse
import json
import io

import imagehash
import cv2
import numpy as np
from PIL import Image 

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StructField, StructType
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except:
    pass

sc._jsc.hadoopConfiguration().set("mapred.max.split.size", "33554432")

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()

with open(args.json, "rb") as handle:
    json_args = json.load(handle)
print(json_args)

POST_PROCESSING_SQL = json_args['sql']
DS = json_args["ds"]
OUTPUT_TABLE= json_args["output_table"]
SRC_TABLE = json_args["src_table"]
ACTIVE_PRODUCTS_TABLE = json_args["active_products_table"]

HEADERS = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7)'}

def hash_binary_image(content):
    b_data = io.BytesIO(content)
    file_bytes = np.asarray(bytearray(b_data.read()), dtype=np.uint8)
    img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR, )[:, :, ::-1]
    pil_image = Image.fromarray(img)
    image_hash = imagehash.dhash(pil_image, hash_size=5).hash.tobytes()
    return image_hash

def downloader(url: str) -> t.Optional[dict]:
    try:
        request = urllib.request.Request(url, headers=HEADERS)
        res = urllib.request.urlopen(request, timeout=5)
        image_content = res.read()
    except:
        return None
    if image_content is None:
        return None
    return {
        "image_content": image_content,
        "image_hash": hash_binary_image(image_content)
    }

downloadUDF = F.udf(
    downloader, 
    StructType([
        StructField('image_content', BinaryType()),
        StructField('image_hash', BinaryType())
    ])
)

sql = f"""
SELECT product_image_url, product_id 
FROM {SRC_TABLE}
WHERE execution_date='{DS}'
AND product_id NOT IN (
    SELECT product_id FROM {ACTIVE_PRODUCTS_TABLE}
)
"""
print(sql)

## DO NOT ADD ANY JOINS
## DESTROYS UDF PARALLELIZATION
sqlContext.sql(sql) \
    .repartition(sc.defaultParallelism * 3) \
    .withColumn("image_data", downloadUDF(F.col("product_image_url"))) \
    .select(["product_id", "image_data.image_content", "image_data.image_hash", 'product_image_url']) \
    .write \
    .option("mergeSchema",True) \
    .saveAsTable(OUTPUT_TABLE, mode="overwrite", format="delta")

