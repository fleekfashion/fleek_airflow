import argparse
import json

from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.column import Column
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
DS = json_args["DS"]

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except Exception:
    pass

## Run SQL
## IMPORTANT!! This deletes product ids that
## we are going to overwrite so we can append
for query in SQL.split(";"):
    df = sqlContext.sql(query)

#############################################################
# Processing: Filter out products whose labels dont match
#############################################################
def sorted_posexplode(score_field: str, additional_fields: list, asc: bool = True
        ) -> Column:
    fields = [score_field] + additional_fields
    zipped_fields = F.arrays_zip(*fields) # score first for sorting
    sorted_fields = F.array_sort(zipped_fields)
    correctly_sorted_fields = sorted_fields if asc else F.reverse(sorted_fields)
    return F.posexplode(correctly_sorted_fields)

df.groupBy("product_id") \
    .agg(
        F.collect_list("similar_product_id").alias("similar_product_ids"),
        F.collect_list("similarity_score").alias("similarity_scores")
    ) \
    .select(
        "product_id",
        sorted_posexplode(
            "similarity_scores",
            ["similar_product_ids"],
            asc=False
        )
    ) \
    .select(
        F.col("product_id"),
        F.col("pos").alias("index"),
        F.col("col.similar_product_ids").alias("similar_product_id"),
        F.col("col.similarity_scores").alias("similarity_score")
    ) \
    .withColumn(
        "execution_date",
        F.lit(DS).cast(DateType())
    ) \
    .where(F.col("index") < TOP_N) \
    .write \
    .option("mergeSchema", "true") \
    .saveAsTable(OUTPUT_TABLE, format="delta", mode="append")
