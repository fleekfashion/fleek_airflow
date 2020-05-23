import os
import tarfile
import argparse 
import shutil

from google.cloud import bigquery as bq
import numpy as np
import pandas as pd
import dill
import tensorflow as tf


parser = argparse.ArgumentParser()
parser.add_argument("--processor_out", type=str, required=True)
parser.add_argument("--model_out", type=str, required=True)
parser.add_argument("--project", type=str, required=True)
args = parser.parse_args()

PROJECT = args.project 
PROC_OUTPUT_PATH = args.processor_out
MODEL_OUTPUT_PATH = args.model_out

c = bq.Client(PROJECT)
query1 = f"""
SELECT product_id, product_embedding
FROM `{PROJECT}.personalization.active_products` 
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
        embeddings[ind] = emb
        ind+=1
    cntr+=1

ind_to_pid = np.zeros(len(pid_to_ind.values()), np.int64)
for pid, ind in pid_to_ind.items():
    ind_to_pid[ind] = pid


tf.keras.backend.clear_session()
class Embedder(tf.keras.layers.Layer):
    def __init__(self, depth):
        super(Embedder, self).__init__()
        self.depth = depth
        
    def call(self, inputs):
        inputs = tf.cast(inputs, tf.int32)
        one_hot = tf.one_hot(inputs, on_value=1.0, 
                             off_value=0.0, depth=self.depth, 
                            dtype=tf.float32, axis=-1)
        encoding = tf.reduce_sum(one_hot, axis=1)
        return encoding

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
        filtered_scores = tf.multiply(scores, tf.cast(inputs == 0, tf.float32) )
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

inputs = tf.keras.layers.Input(shape=(None,), dtype=tf.int32)
embedded = Embedder(depth=len(pid_to_ind))(inputs)
scores = Recommender(encoder=encoder, decoder=decoder, n_active=n_active)(embedded)
top_n_scores = TopN(10, argsort=False)(scores)
top_n_args = TopN(10, argsort=True)(scores)
model = tf.keras.models.Model(inputs=inputs, 
                              outputs={
                                  "top_scores": top_n_scores, 
                                  "top_inds": top_n_args,
                              }
                             )
                 
# TF function for serving
@tf.function()
def serve_predict(user_product_interactions):
    prediction = model(user_product_interactions)
    return prediction
serve_predict = serve_predict.get_concrete_function(user_product_interactions=tf.TensorSpec( 
    shape=model.inputs[0].shape, 
    dtype=model.inputs[0].dtype, 
    name="user_product_interactions") 
)


def preprocessing(user_product_interactions: list) -> list:
    values = [] 
    for i, up in enumerate(user_product_interactions):
        inds  = []
        for pid in up:
            inds.append( pid_to_ind.get(pid, 0) )
        values.append(inds)
    return values

def postprocessing(top_inds: list) -> list:
    output = np.zeros(shape=(len(top_inds), len(top_inds[0]) ), dtype=np.int64)
    for i, row in enumerate(top_inds):
        output[i] = ind_to_pid[row]
    return output.tolist()

## Save functions
with open(PROC_OUTPUT_PATH, "wb") as handle:
    dill.dump({
        "preprocessing": preprocessing,
        "postprocessing": postprocessing
    }, handle)

    MODEL_BASE_PATH = os.path.dirname(MODEL_OUTPUT_PATH) 
    MODEL_FOLDER_PATH = MODEL_BASE_PATH + "/model"
    os.mkdir(MODEL_FOLDER_PATH)

model.save(MODEL_FOLDER_PATH+"/1", save_format="tf", signatures={
            tf.saved_model.DEFAULT_SERVING_SIGNATURE_DEF_KEY : serve_predict
    } 
)

with tarfile.open(f"{MODEL_OUTPUT_PATH}", "w:gz") as tar:
    tar.add(MODEL_FOLDER_PATH,
            arcname=os.path.basename(MODEL_FOLDER_PATH))
shutil.rmtree(MODEL_FOLDER_PATH)
