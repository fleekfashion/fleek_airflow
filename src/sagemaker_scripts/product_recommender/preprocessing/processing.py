import os
import argparse
import pickle
import jsonlines
import numpy as np
from google.cloud import bigquery as bq

parser = argparse.ArgumentParser()
parser.add_argument("--project", type=str, required=True)
parser.add_argument("--main_out", type=str, required=True)
parser.add_argument("--uid_out", type=str, required=True)
parser.add_argument("--processor_path", type=str, required=True)
args = parser.parse_args()
print(args)

PROJECT = args.project
MAIN_OUT = args.main_out
USER_IDS_OUT = args.uid_out
PROCESSOR_PATH = args.processor_path

c = bq.Client(project=PROJECT)

df = c.query(
f"""
    SELECT 
        user_id,
        product_ids,
        weights
    FROM `fleek-prod.personalization.aggregated_user_data`
"""

).result().to_dataframe()

with open(PROCESSOR_PATH, 'rb') as handle:
    proc = pickle.load(handle)
    kv = proc['pid_to_ind']

user_ids = df.user_id.tolist()
inds = df.product_ids.apply(lambda pids: [kv.get(pid, 0) for pid in pids]).tolist()
weights = df.weights.tolist()

data = np.zeros([len(user_ids), len(kv) ])
for i, (ind, ws) in enumerate(zip(inds, weights)):
    data[i, ind] = ws
data = data.tolist()

with open(MAIN_OUT, "w+") as handle:
    w = jsonlines.Writer(handle)
    w.write_all(data)
    w.close()
                
with open(USER_IDS_OUT, "w+") as handle:
    w = jsonlines.Writer(handle)
    w.write_all(user_ids)
    w.close()
