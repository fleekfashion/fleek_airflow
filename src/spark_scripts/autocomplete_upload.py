import argparse
import json
import copy
import re
from typing import Set, List

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

import pandas as pd
import numpy as np
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


## Required args
SQL = json_args["sql"]
SEARCH_URL = json_args['search_url']
SEARCH_PASSWORD = json_args['search_password']
AUTOCOMPLETE_INDEX = json_args['autocomplete_index']
ACTIVE_PRODUCTS_TABLE = json_args['active_products_table']


c = meilisearch.Client(SEARCH_URL, SEARCH_PASSWORD)
index = c.get_index(AUTOCOMPLETE_INDEX)

advertisers = spark \
    .sql(f"SELECT DISTINCT(advertiser_name) \
    FROM {ACTIVE_PRODUCTS_TABLE} ORDER BY advertiser_name") \
    .collect()
ADVERTISER_NAMES = seq(advertisers) \
    .map(lambda x: x.advertiser_name) \
    .make_string(",_,")

# Run SQL
for query in SQL.split(";"):
    df = sqlContext.sql(query)

pdf = df.toPandas()
pdf['advertiser_names'] = ADVERTISER_NAMES
pdf['colors'] = ""
pdf['primary_key'] = pdf.index
final_docs = pdf.to_dict('records')


index.delete_all_documents()
step = 1000
for i in range(0, len(final_docs) , step):
    res = index.add_documents(final_docs[i: min(i+step, len(final_docs))])
