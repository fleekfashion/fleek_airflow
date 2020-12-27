import argparse
import json

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

# Databricks notebook source
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

OUTPUT_TABLE = json_args["output_table"]
TOP_N = json_args["TOP_N"]
SQL = json_args["sql"]

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except Exception:
    pass

## Run SQL
for query in SQL.split(";"):
    df = sqlContext.sql(query)

#############################################################
# Processing: Filter out products whose labels dont match
#############################################################
df.groupBy("product_id") \
    .agg(
        F.collect_list("similar_product_id").alias("similar_product_ids"),
        F.collect_list("similarity_score").alias("similarity_scores")
    ) \
    .select(
        "product_id",
        F.posexplode(
            F.reverse(
                F.array_sort(
                    F.arrays_zip(
                        "similarity_scores",
                        "similar_product_ids"
                    )
                )
            ),
        )
    ) \
    .select(
        F.col("product_id"),
        F.col("pos").alias("index"),
        F.col("col.similar_product_ids").alias("similar_product_id"),
        F.col("col.similarity_scores").alias("similarity_score")
    ) \
    .where(F.col("index") < TOP_N) \
    .write.saveAsTable(OUTPUT_TABLE, format="delta", mode="append")
