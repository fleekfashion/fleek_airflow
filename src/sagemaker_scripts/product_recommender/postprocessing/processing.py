import argparse
import pickle
import json

import numpy as np
import jsonlines
import pandas as pd
from google.cloud import bigquery as bq

parser = argparse.ArgumentParser()
parser.add_argument("--main_input_path", type=str, required=True)
parser.add_argument("--id_input_path", type=str, required=True)
parser.add_argument("--project", type=str, required=True)
parser.add_argument("--bq_output_table", type=str, required=True)
parser.add_argument("--processor_path", type=str, required=True)
args = parser.parse_args()

PROJECT = args.project
MAIN_INPUT_PATH = args.main_input_path
ID_INPUT_PATH = args.id_input_path
PROCESSOR_PATH = args.processor_path
BQ_OUTPUT_TABLE = args.bq_output_table

with open(PROCESSOR_PATH, 'rb') as handle:
    IND_TO_PID = np.array(pickle.load(handle)["ind_to_pid"])
    
def get_sagemaker_line(fp):
    data_f = open(fp, 'r')
    cur_str = ""
    line = data_f.readline()
    while line:
        cur_str += line
        if line[0] == "}":
            data = json.loads(cur_str)
            cur_str = ""
            yield data
        line = data_f.readline()
    data_f.close()
   
def get_df(top_product_ids, top_product_scores, ids):
    top_n = top_product_ids.shape[1]
    out_data = {"user_id": ids}
    for i in range(top_n):
        out_data[f"top_product_ids_{i}"] = top_product_ids[:, i]
    for i in range(top_n):
        out_data[f"top_product_scores_{i}"] = top_product_scores[:, i]
    df = pd.DataFrame(out_data)
    return df
    
def get_batch_rec(main_input_path, id_input_path):
    id_f = open(id_input_path, 'r')
    reader = jsonlines.Reader(id_f).iter()
    ids = []
    for r in reader:
        ids.append(r)

    generator  = get_sagemaker_line(main_input_path)
    
    id_pos = 0
    for batch in generator:
        top_product_scores = []
        top_product_ids = []
        batch_ids = []
        for b in batch["predictions"]:
            top_pscores = b["top_scores"]
            top_pids = IND_TO_PID[b["top_inds"]]

            top_product_scores.append(top_pscores)
            top_product_ids.append(top_pids)
            batch_ids.append(ids[id_pos])
            id_pos+=1
            
        top_product_scores = np.array(top_product_scores)
        top_product_ids = np.array(top_product_ids)
        df = get_df(top_product_ids, top_product_scores, batch_ids)
        yield df
    id_f.close()       

generator = get_batch_rec(MAIN_INPUT_PATH, ID_INPUT_PATH)
for df in generator:
    c = bq.Client(PROJECT)
    job_config = bq.LoadJobConfig(write_disposition="WRITE_APPEND")
    c.load_table_from_dataframe(df, BQ_OUTPUT_TABLE, job_config=job_config).result()
