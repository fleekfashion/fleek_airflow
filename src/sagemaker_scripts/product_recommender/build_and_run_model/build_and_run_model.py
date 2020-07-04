import argparse

from google.cloud import bigquery as bq
import numpy as np
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("--project", type=str, required=True)
parser.add_argument("--top_n", type=int, required=True)
parser.add_argument("--bq_output_table", type=str, required=True)
args = parser.parse_args()
print(args)

PROJECT = args.project
TOP_N = args.top_n
BQ_OUTPUT_TABLE = args.bq_output_table

c = bq.Client(PROJECT)

# Get active products
query1 = f""" SELECT product_id, product_embedding
FROM `{PROJECT}.personalization.active_products` 
"""
active_data = c.query(query1).result()
a_df = active_data.to_dataframe()

# Get historic products
query2 = f"""
SELECT product_id, product_embedding
FROM `{PROJECT}.personalization.historic_products`
WHERE execution_date > DATE_ADD( CURRENT_DATE(), INTERVAL -90 DAY ) 
"""
historic_data = c.query(query2).result()
h_df = historic_data.to_dataframe()
n_active = len(a_df.product_id)
df = pd.concat( [a_df, h_df], ignore_index=True)

## TODO
## ind starts at 1 and we add 1 row to the emb matrix 
## so that the 0th index is a 0 embedding. We will use
## this for any product_ids that we do not recognize 
embeddings = np.zeros( [ len(df.product_id.unique()) + 1, len(df.product_embedding[0]) ], dtype=np.float32 )
pid_to_ind = {0:0}
ind = 1
cntr = 0
while cntr < len(df):
    pid, emb = df.product_id[cntr], df.product_embedding[cntr]
    if pid in pid_to_ind:
        pass
    else:
        pid_to_ind[pid] = ind
        embeddings[ind] = emb
        ind+=1
    cntr+=1
ind_to_pid = np.zeros(len(pid_to_ind.values()), np.int64)
for pid, ind in pid_to_ind.items():
    ind_to_pid[ind] = pid

# Get user data
df = c.query(
f"""
    SELECT 
        user_id,
        product_ids,
        weights
    FROM `fleek-prod.personalization.aggregated_user_data`
"""

).result().to_dataframe()

## Parse user data into matrix
user_ids = df.user_id.tolist()
inds = df.product_ids.apply(lambda pids: [pid_to_ind.get(pid, 0) for pid in pids]).tolist()
weights = df.weights.tolist()
data = np.zeros([len(user_ids), len(pid_to_ind) ])
for i, (ind, ws) in enumerate(zip(inds, weights)):
    data[i, ind] = ws

###########################
## Run Recommendations
###########################

# Go from data -> emb -> pred
user_emb = np.matmul(data, embeddings)
normalized_user_emb = user_emb / np.linalg.norm(user_emb, axis=1)[:, None]
emb_inv = np.linalg.pinv(embeddings)
scores = np.matmul(normalized_user_emb, embeddings.T)

# Filter out historical product
scores = scores[:, :n_active]

# Add a little randomness to scores
probablistic_scores = np.random.normal(scores, scale=.05)

## Get top pids
top_inds = np.argsort(probablistic_scores)[:, ::-1][:, :TOP_N]
top_pids = np.array(list(map( lambda x: ind_to_pid[x], top_inds.tolist())))

## Convert to DF
df = pd.DataFrame()
df["user_id"] = user_ids
for i in range(len(top_pids[0])):
    df[f"top_product_ids_{i}"] = top_pids[:, i]

job_config = bq.LoadJobConfig(write_disposition="WRITE_APPEND")
c.load_table_from_dataframe(df, BQ_OUTPUT_TABLE, job_config=job_config).result()
