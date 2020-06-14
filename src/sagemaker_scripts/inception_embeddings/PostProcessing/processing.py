import argparse

import numpy as np
import json
import jsonlines
import pandas as pd
from google.cloud import bigquery as bq

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True)
parser.add_argument("--pid_input_path", type=str, required=True)
parser.add_argument("--bq_output_table", type=str, required=True)
args = parser.parse_args()

input_path = args.input_path
pid_input_path = args.pid_input_path
bq_output_table = args.bq_output_table

data_f = open(input_path, 'r')
pid_f = open(pid_input_path, 'r')

reader = jsonlines.Reader(pid_f).iter()
pids = []
for r in reader:
    pids.append(r)

data_f = data_f.read()
data = data_f.replace("{\n    ", "{").replace("]\n", "]").split("\n")

predictions = []
for batch_string in data:
    if len(batch_string):
        batch = json.loads(batch_string)['predictions']
        predictions.extend(batch)

output = {}
predictions = np.array(predictions)
for i in range(predictions.shape[-1]):
    output[f"emb_{i}"] = predictions[:, i]
output["product_id"] = pids
df = pd.DataFrame(output)
print(df.head())

c = bq.Client("fleek-prod")
job_config = bq.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
c.load_table_from_dataframe(df, bq_output_table, job_config=job_config).result()
