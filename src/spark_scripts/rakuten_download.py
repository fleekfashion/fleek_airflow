import argparse
import json
from pyspark.sql import SQLContext

## Hack for linter
try:
    sqlContext = SQLContext(1)
except:
    pass

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

## Required args
VALID_ADVERTISERS = json_args['valid_advertisers']
OUTPUT_TABLE = json_args['output_table']

