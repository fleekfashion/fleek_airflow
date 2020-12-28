import argparse
import json

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

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

