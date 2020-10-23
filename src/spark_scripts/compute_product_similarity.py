import argparse
import json

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
import numpy as np

# Databricks notebook source
from pyspark.mllib.linalg import Matrix, Matrices
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, BlockMatrix
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Row

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

ACTIVE_TABLE = json_args["active_table"]
HISTORIC_TABLE = json_args["historic_table"]
OUTPUT_TABLE = json_args["output_table"]
TOP_N = json_args["TOP_N"]
SQL = json_args["sql"]

LIMIT = json_args.get('limit')

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except:
    pass

######################################################
# Build Local Active Matrix
######################################################
active_df = sqlContext.table(ACTIVE_TABLE) \
        .select(["product_id", "product_image_embedding"])

active_data = active_df.select(["product_id", "product_image_embedding"]).collect()
ind_to_pid = np.zeros(len(active_data), np.int64)
embs = []
for ind, value in enumerate(active_data):
    v = value.asDict()
    ind_to_pid[ind] = v["product_id"]
    embs.append(v["product_image_embedding"])

embs = np.array(embs)
N_ACTIVE = embs.shape[0]
N_DIMS = embs.shape[1]
active_matrix = Matrices.dense(numCols=N_ACTIVE, numRows=N_DIMS, values=embs.flatten())

######################################################
# Build Distributed Historic Matrix 
######################################################
historic_df = sqlContext.table(HISTORIC_TABLE) \
    .select(["product_id", "product_image_embedding"])
product_df = active_df.union(historic_df) \
    .drop_duplicates(subset=["product_id"])

## Quick testing
if LIMIT is not None:
    product_df = product_df.limit(LIMIT)

indexed_df = product_df.rdd.map(lambda row: IndexedRow(row.product_id, row.product_image_embedding))
indexed_matrix = IndexedRowMatrix(rows=indexed_df)


######################################################
# Compute Similarity Matrix 
######################################################
similarity = indexed_matrix.multiply(active_matrix)
res_df = similarity.rows.map(lambda x: Row(x.index, x.vector.toArray().tolist())).toDF(["product_id", "score"])


######################################################
# UDFS to Convert Index to Product ID 
######################################################
ind_pid = [ Row(ind=ind, pid=pid) for ind, pid in enumerate(ind_to_pid.tolist())]
spark.createDataFrame(ind_pid).createOrReplaceTempView("ind_pid_map")

#############################################################
# Transformations: Sort products by scores 
#############################################################
res_df.withColumn("scoreIndex", F.sequence(F.lit(0), F.lit(embs.shape[0]))) \
  .withColumn("indexedScores", F.arrays_zip("score", "scoreIndex")) \
  .withColumn("topScores", F.slice(F.reverse(F.array_sort("indexedScores")), 2, TOP_N)) \
  .select(F.col("product_id"), F.posexplode(F.col("topScores"))) \
  .select(
      F.col("product_id"),
      F.col("col.score").alias("similarity_score"),
      F.col("col.scoreIndex").alias("scoreIndex")
  ).createOrReplaceTempView("preindexed_similar_products")

#############################################################
# Transformations: Replace score index with product_id
#############################################################
sqlContext.sql("""
SELECT si.product_id, si.similarity_score, ipm.pid as similar_product_id 
FROM ind_pid_map ipm
INNER JOIN preindexed_similar_products si 
ON si.scoreIndex=ipm.ind
""").write.saveAsTable(OUTPUT_TABLE, format="delta", mode="overwrite")
