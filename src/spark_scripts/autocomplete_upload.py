import argparse
import json
import copy
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

TAXONOMY_PATH = json_args['taxonomy_path']
COLORS_PATH = json_args['colors_path']
GLOBAL_ATTRIBUTES_PATH = json_args['global_attributes_path']
LABELS_PATH = json_args['labels_path']
SEARCH_URL = json_args['search_url']
SEARCH_PASSWORD = json_args['search_password']
AUTOCOMPLETE_INDEX = json_args['autocomplete_index']
PRODUCT_SEARCH_INDEX = json_args['product_search_index']
ACTIVE_PRODUCTS_TABLE = json_args['active_products_table']

with open(TAXONOMY_PATH, 'r') as handle:
    docs = json.load(handle)
with open(GLOBAL_ATTRIBUTES_PATH, 'r') as handle:
    global_attribute_map = json.load(handle)
with open(LABELS_PATH, 'r') as handle:
    LABELS = list(json.load(handle).keys())
with open(COLORS_PATH, 'r') as handle:
    COLORS = json.load(handle)

advertisers = spark \
    .sql(f"SELECT DISTINCT(advertiser_name) \
    FROM {ACTIVE_PRODUCTS_TABLE} ORDER BY advertiser_name") \
    .collect()
ADVERTISER_NAMES = seq(advertisers) \
    .map(lambda x: x.advertiser_name) \
    .to_list()


c = meilisearch.Client(SEARCH_URL, SEARCH_PASSWORD)
index = c.get_index(AUTOCOMPLETE_INDEX)
product_search_index = c.get_index(PRODUCT_SEARCH_INDEX)

################################################
# Process Each row: handle nulls, filters etc.
################################################
def _process_doc(doc):
    doc = copy.copy(doc)
    doc['attribute_descriptor'] = [""] + doc.get('attribute_descriptor', [])    
    doc['product_hidden_label'] = doc.get('product_hidden_label', "")
    doc['product_label'] = doc.get('product_label', [])
    doc['advertiser_names'] = ",_,".join(ADVERTISER_NAMES)
    doc['colors'] = ",_,".join(COLORS)
    doc["is_base_label"] = doc.get("is_base_label", False)
    doc["secondary_attributes"] = doc.get("secondary_attributes", [])
    doc["primary_attribute"] = doc.get("primary_attribute", "")
    doc["product_label"] = [""] + doc['product_label'] if not doc['is_base_label'] else doc["product_label"]
    return doc

################################################
# EXPLODE dataframe and Add global attributes 
################################################
processed_docs = (
        seq(docs) + 
        seq(LABELS).map(
            lambda x: {
                "product_label": [x],
                "is_base_label": True
            }
        )
    ) \
    .map(_process_doc) \
    .to_list()
df = pd.DataFrame(processed_docs)

def posexplode(df, attribute):
    df = df.explode(attribute)
    df[attribute + '_rank'] = df.groupby(df.index).cumcount()+1
    df = df.reset_index(drop=True)
    return df
def _add_global_attributes(doc):
    existing_attributes = doc['secondary_attributes'] 
    other_fields = [doc['primary_attribute']] + doc['attribute_descriptor']
    
    global_attributes = seq(global_attribute_map.get(doc['product_label'], [])) \
        .filter(lambda x: x not in existing_attributes + other_fields) \
        .to_list()
    doc['secondary_attribute'] = [""] + existing_attributes + global_attributes
    return doc


df = posexplode(df, "product_label") \
    .apply(_add_global_attributes, axis=1) \
    .drop(labels=['secondary_attributes'], axis=1)
df = posexplode(df, "secondary_attribute")
df = posexplode(df, "attribute_descriptor")
df = df.drop_duplicates()
df['rank'] = (1.1*df.product_label_rank + 
              df.secondary_attribute_rank +
              df.attribute_descriptor_rank) / 3.1
df['primary_key'] = df.index


################################################
# Filter out autocomplete string with no items
################################################

def _has_hits(doc) -> bool:
    query = f"{doc['secondary_attribute']} {doc['primary_attribute']} {doc['attribute_descriptor']}".rstrip().lstrip()
    label = doc.get('product_label', "")
    facetFilters = [f"product_labels:{label}"] if len(label) > 0 else None
    res = product_search_index.search(query, opt_params={
        "offset":0,
        "limit":1,
        "attributesToRetrieve": ["product_id"],
        "facetFilters": facetFilters
    })
    return len(res['hits']) > 0
exploded_docs = df.to_dict(orient='records')
final_docs = seq(exploded_docs) \
    .filter(_has_hits) \
    .to_list()


################################################
# Delete inactive documents
################################################
def get_keys_to_delete(active_ids: Set[int], primary_key: str) -> List[int]:
    hits = index.search("", opt_params={
        "offset": 0,
        "limit": 10**10,
        "attributesToRetrieve": [primary_key]
    })['hits']
    old_keys = seq(hits) \
            .map(lambda x: x[primary_key]) \
            .filter(lambda x: x not in active_ids) \
            .to_list()
    return old_keys

index.delete_documents(get_keys_to_delete(
    active_ids=seq(final_docs).map(lambda x: x['primary_key']).to_set(),
    primary_key='primary_key'
))

################################################
# Batch upload documents 
################################################
step = 1000
for i in range(0, len(final_docs) , step):
    res = index.add_documents(final_docs[i: min(i+step, len(final_docs))])
