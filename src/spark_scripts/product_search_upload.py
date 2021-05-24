import argparse
import copy
import json
from typing import List, Dict, Set

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

import meilisearch
from functional import seq

## Hack for linter
try:
    sqlContext = SQLContext(1)
    sc = SparkContext()
    spark = SparkSession(sc)
except:
    pass

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

SQL = json_args["sql"]
SEARCH_ENDPOINT = json_args["search_endpoint"]
FIELDS = json_args["fields"]
SEARCH_URL = json_args["search_url"]
SEARCH_PASSWORD = json_args["search_password"]

c = meilisearch.Client('http://161.35.113.38/', 'fleek-app-prod1')
index = c.get_index(SEARCH_ENDPOINT)

def rows_to_dicts(l: list) -> list:
    if l is None:
        return None
    res = seq(l) \
      .map(lambda x: x.asDict()) \
      .to_list()
    return res

def _process_entry(e):
    e = copy.copy(e)
    e['execution_date'] = e['execution_date'].strftime('%Y-%m-%d')
    e['sizes'] = rows_to_dicts(e['sizes'])
    e['product_color_options'] = rows_to_dicts(e['product_color_options'])
    return e

def get_keys_to_delete(active_ids: Set[int]) -> List[int]:
    hits = index.search("", opt_params={
        "offset": 0,
        "limit": 10**10,
        "attributesToRetrieve": ["product_id"]
    })['hits']
    old_keys = seq(hits) \
            .map(lambda x: x['product_id']) \
            .filter(lambda x: x not in active_ids) \
            .to_list()
    top_keys = hits[:5000]
    return old_keys + top_keys
    
## Load current data
df = spark.sql(SQL) \
        .select(FIELDS)

## Collect Data
data = seq(df.collect()) \
        .map(lambda x: x.asDict()) \
        .map(_process_entry) \
        .to_list()
active_product_ids = seq(data) \
        .map(lambda x: x['product_id']) \
        .to_set()

try:
    ## Delete products that are no longer active
    index.delete_documents(get_keys_to_delete(active_product_ids))
except:
    print("FAILED TO DELETE DOCS")

## Upload Products
step = 300
for i in range(0, len(data) , step):
    res = index.add_documents(data[i: min(i+step, len(data) - 1)])
