import urllib.request
import shutil# Download URL and save to outpath.
import os
import pathlib
import argparse
import json

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

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
print(json_args)

DS = json_args["ds"]
OUTPUT_TABLE= json_args["output_table"]
SRC_TABLE = json_args["src_table"]
ACTIVE_PRODUCTS_TABLE = json_args["active_products_table"]

def downloader(url):
    try:
        request = urllib.request.urlopen(url, timeout=40)
        data = request.read()
    except:
        return None
    return data

sql = f"""
SELECT product_image_url, product_id 
FROM {SRC_TABLE}
WHERE execution_date='{DS}'
AND product_id NOT IN (
    SELECT product_id FROM {ACTIVE_PRODUCTS_TABLE}
)
"""
print(sql)

downloadUDF = F.udf(downloader, BinaryType())
sqlContext.sql(sql).cache() \
    .repartition(sc.defaultParallelism * 3) \
    .withColumn("image_content", downloadUDF(F.col("product_image_url"))) \
    .select(["product_id", "image_content"]) \
    .dropna() \
    .write.saveAsTable(OUTPUT_TABLE, mode="overwrite", format="delta")
