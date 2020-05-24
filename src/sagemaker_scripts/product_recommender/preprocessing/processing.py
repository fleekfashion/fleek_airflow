import os
import argparse
import jsonlines
import numpy as np
from google.cloud import bigquery

parser = argparse.ArgumentParser()
parser.add_argument("--project", type=str, required=True)
parser.add_argument("--main_out", type=str, required=True)
parser.add_argument("--uid_out", type=str, required=True)
args = parser.parse_args()

PROJECT = args.project
MAIN_OUT = args.main_out
USER_IDS_OUT = args.uid_out

c = bigquery.Client(PROJECT)

sql = f"SELECT DISTINCT(product_id) FROM `{PROJECT}.personalization.active_products`"
df = c.query(sql).result().to_dataframe()
pids = df.product_id.tolist()

uids = [1, 2, 3]
user_data = [
    [pids[0], pids[1] ],
    [pids[2], pids[3], pids[4] ],
    [pids[5] ]
]

max_len = 0
for row in user_data:
    max_len = max(max_len, len(row) )

user_data = [
    [1, 2],
    [3, 4, 5],
    [6]
]

data = np.zeros([len(user_data), max_len ], dtype=np.int32)
for i, row in enumerate(user_data):
    for j, d in enumerate(row):
        data[i, j] = d
data = data.tolist()

with open(MAIN_OUT, "w+") as handle:
    w = jsonlines.Writer(handle)
    w.write_all(data)
    w.close()
                
with open(USER_IDS_OUT, "w+") as handle:
    w = jsonlines.Writer(handle)
    w.write_all(uids)
    w.close()

