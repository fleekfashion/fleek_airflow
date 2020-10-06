import argparse
import json

import pyspark.sql.functions as F
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark.mllib.linalg import Matrix, Matrices
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, BlockMatrix
from pyspark.sql import Row
from pyspark.sql.types import *

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

ACTIVE_TABLE = json_args["active_table"]
HISTORIC_TABLE = json_args["historic_table"]
EVENTS_TABLE = json_args["events_table"]
OUTPUT_TABLE = json_args["output_table"]
TOP_N = json_args["TOP_N"]

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except:
    pass

TRASHED_EVENT = "trashed_item"
FAVED_EVENT = "faved_item"
BAGGED_EVENT = "bagged_item"

# COMMAND ----------

TOP_N = 500

# COMMAND ----------

events_df = sqlContext.table(EVENTS_TABLE).filter("execution_date>'2020-09-24'") \
  .where(f"event in ('{TRASHED_EVENT}', '{BAGGED_EVENT}', '{FAVED_EVENT}')") \
  .where("product_id is NOT NULL")
active_df = sqlContext.table(ACTIVE_TABLE).select("product_id", "product_image_embedding")
historic_df = sqlContext.table(HISTORIC_TABLE).select("product_id", "product_image_embedding")
product_df = active_df.union(historic_df)

########################################################
## Generate Product Interaction Scores
#######################################################
def time_decay(score):
    days_ago = F.datediff(F.current_date(), F.col("execution_date"))
    return score / days_ago
  
def get_base_positive_score():
    event = F.col("event")
    method = F.col("method")
    faved =  F.when(event == F.lit(FAVED_EVENT), 1.0)
    bagged = F.when(event == F.lit(BAGGED_EVENT), 3.0)
    return F.coalesce(faved, bagged).cast("float") 

def get_base_negative_score():
    event = F.col("event")
    method = F.col("method")
    trashed =  F.when(event == F.lit(TRASHED_EVENT), -1.0)
    return trashed.cast("float")
        
denormalized_df = events_df .withColumn("positiveScore", time_decay(get_base_positive_score())) \
  .withColumn("negativeScore", time_decay(get_base_negative_score())) \
  .alias("denormalized_df")

########################################################
## User level negative weighting factor
#######################################################
ratio_df = denormalized_df.groupBy("user_id").agg(
  F.sum(F.col("positiveScore")).alias("sumPositiveScore"),
  F.sum(F.col("negativeScore")).alias("sumNegativeScore"),
  F.collect_set(F.col("product_id")).alias("interaction_product_ids")) \
  .where("abs(sumNegativeScore)>20") \
  .where("sumPositiveScore>5") \
  .withColumn("negativeNormalizationFactor",
    F.abs(F.col("sumPositiveScore")/F.col("sumNegativeScore")) ) \
  .alias("ratio_df")

normalized_df = denormalized_df.join(ratio_df, 
  denormalized_df["user_id"] == ratio_df["user_id"], "inner" ) \
  .select("denormalized_df.*", "ratio_df.negativeNormalizationFactor") \
  .withColumn("negativeScore", F.lit(.8)*F.col("negativeScore")*F.col("negativeNormalizationFactor")) \
  .alias("normalized_df")


########################################################
## User Embedding 
#######################################################
embeddings_df = normalized_df.join(product_df, normalized_df['product_id'] == product_df['product_id'], "inner") \
  .select("normalized_df.*", "product_image_embedding") \
  .withColumn("score", F.coalesce("positiveScore", "negativeScore")) \
  .withColumn("normalizedEmbedding", F.expr("TRANSFORM(product_image_embedding, x -> x*score )"))

n = len(product_df.select("product_image_embedding").first()[0])
user_embeddings_df = embeddings_df.groupBy("user_id").agg(
    F.array(*[F.avg(F.col("normalizedEmbedding")[i]) for i in range(n)]).alias("userEmbedding") 
) \
  .withColumn("arrayMagnitude", 
    F.expr("aggregate(userEmbedding, cast(0 as double), (acc, x) -> acc+x*x, acc -> sqrt(acc) )" )) \
  .withColumn("userEmbedding", F.expr("transform(userEmbedding, x -> x/arrayMagnitude)"))

######################################################
# Build Local Active Matrix
######################################################
active_data = active_df.collect()
ind_to_pid = np.zeros(len(active_data), np.int64)
pid_to_ind = { pid: ind for ind, pid in enumerate(ind_to_pid) }
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
# Compute Similarity Matrix 
######################################################
user_df = user_embeddings_df.rdd.map(lambda row: IndexedRow(row.user_id, row.userEmbedding))
indexed_user_matrix = IndexedRowMatrix(rows=user_df)
similarity = indexed_user_matrix.multiply(active_matrix)
res_df = similarity.rows.map(lambda x: Row(x.index, x.vector.toArray().tolist())).toDF(["user_id", "score"]).alias("res_df")

######################################################
# UDFS to Process Scores: ind_to_pid and remove seen pids 
######################################################
broadcast_map = sc.broadcast(ind_to_pid.tolist())

def processScores(scores_and_inds, interaction_pids):
    seen_pids = set(interaction_pids)  
    m = broadcast_map.value
    def _replace_index_with_pid(elem):
        return Row(score=elem[0], product_id=m[elem[1]-1])
    scores_and_pids = map(_replace_index_with_pid, scores_and_inds)
    return list(filter(lambda x: x[1] not in seen_pids, scores_and_pids))
  
processScoresUDF = F.udf(processScores, ArrayType(StructType([
  StructField("score", DoubleType()),
  StructField("product_id", LongType())
  ]))
)

#############################################################
# Transformations: Go from scores matrix to top product ids
#############################################################
df = res_df.join(ratio_df, res_df["user_id"] == ratio_df["user_id"]) \
  .select("res_df.*", "interaction_product_ids") \
  .withColumn("index", F.sequence(F.lit(0), F.lit(embs.shape[0]))) \
  .withColumn("indexedScores", F.arrays_zip("score", "index")) \
  .withColumn("processedScores", processScoresUDF(F.col("indexedScores"), F.col("interaction_product_ids"))) \
  .withColumn("topScores", F.slice(F.reverse(F.array_sort("processedScores")), 1, TOP_N)) \
  .withColumn("top_product_ids", F.expr("TRANSFORM(topScores, x -> x['product_id'])")) \
  .select("user_id", "top_product_ids", "topScores")

#############################################################
# Save Recommendations in table 
#############################################################
df.write.saveAsTable(OUTPUT_TABLE, format="delta", mode="overwrite")
