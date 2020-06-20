import os
import tarfile
import argparse
import shutil
import pickle

from google.cloud import bigquery as bq
import numpy as np
import pandas as pd
import tensorflow as tf


parser = argparse.ArgumentParser()
parser.add_argument("--processor_out", type=str, required=True)
parser.add_argument("--model_out", type=str, required=True)
parser.add_argument("--project", type=str, required=True)
parser.add_argument("--top_n", type=int, required=True)
args = parser.parse_args()
print(args)

PROJECT = args.project
PROC_OUTPUT_PATH = args.processor_out
MODEL_OUTPUT_PATH = args.model_out
TOP_N = args.top_n

c = bq.Client(PROJECT)
query1 = f"""
SELECT product_id, product_embedding
FROM `{PROJECT}.personalization.active_products` 
LIMIT 100
"""
active_data = c.query(query1).result()
a_df = active_data.to_dataframe()

query2 = """
SELECT product_id, product_embedding
FROM `fleek-prod.personalization.historic_products`
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
        print(emb)
        embeddings[ind] = emb
        ind+=1
    cntr+=1

ind_to_pid = np.zeros(len(pid_to_ind.values()), np.int64)
for pid, ind in pid_to_ind.items():
    ind_to_pid[ind] = pid


tf.keras.backend.clear_session()
class Recommender(tf.keras.layers.Layer):
    def __init__(self, encoder, decoder, n_active):
        super(Recommender, self).__init__()
        self.V = tf.constant(value=encoder)
        self.Vt = tf.constant(value=decoder)
        self.n_active = n_active
        
    def call(self, inputs):
        user_emb = tf.matmul(inputs, self.V)
        normalized_emb = tf.math.l2_normalize(user_emb, axis=1)
        scores = tf.matmul(normalized_emb, self.Vt)

        ## Set scores of seen products to an extremely negative value
        filtered_scores = tf.multiply(scores, tf.cast(inputs == 0, tf.float32) )
        filtered_scores -= tf.cast(inputs != 0, tf.float32)*100000000
        filtered_scores = filtered_scores[:, :self.n_active]
        return filtered_scores

class TopN(tf.keras.layers.Layer):
    def __init__(self, N, argsort):
        super(TopN, self).__init__()
        self.N = N
        self.argsort = argsort
        
    def call(self, inputs):
        if self.argsort:
            top_n = tf.argsort(inputs, direction="DESCENDING")
        else:
            top_n = tf.sort(inputs, direction="DESCENDING")
        return top_n[:, :self.N]


## Build Model
encoder = embeddings
decoder = embeddings.T

inputs = tf.keras.layers.Input(shape=(len(pid_to_ind),), dtype=tf.float32)
scores = Recommender(encoder=encoder, decoder=decoder, n_active=n_active)(inputs)
top_n_scores = TopN(TOP_N, argsort=False)(scores)
top_n_args = TopN(TOP_N, argsort=True)(scores)
model = tf.keras.models.Model(inputs=inputs, 
                              outputs={
                                  "top_scores": top_n_scores,
                                  "top_inds": top_n_args,
                              }
                             )
## Save functions
with open(PROC_OUTPUT_PATH, "wb") as handle:
    pickle.dump({
        "ind_to_pid": ind_to_pid.tolist(),
        "pid_to_ind": pid_to_ind
    }, handle)

MODEL_BASE_PATH = os.path.dirname(MODEL_OUTPUT_PATH) 
MODEL_FOLDER_PATH = MODEL_BASE_PATH + "/model"
if os.path.exists(MODEL_FOLDER_PATH) and os.path.isdir(MODEL_FOLDER_PATH):
    shutil.rmtree(MODEL_FOLDER_PATH)

os.mkdir(MODEL_FOLDER_PATH)
model.save(MODEL_FOLDER_PATH+"/1", save_format="tf")
with tarfile.open(f"{MODEL_OUTPUT_PATH}", "w:gz") as tar:
    tar.add(MODEL_FOLDER_PATH,
            arcname=os.path.basename(MODEL_FOLDER_PATH))
#shutil.rmtree(MODEL_FOLDER_PATH)
