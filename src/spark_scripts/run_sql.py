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
SQL = json_args["sql"]

## Optional args
MODE = json_args.get("mode")
OUTPUT_TABLE = json_args.get("output_table")

# Run SQL
df = sqlContext.sql(SQL)

if MODE == "WRITE_APPEND":
  df.write.saveAsTable(OUTPUT_TABLE, format="delta", mode="append")
elif MODE == "WRITE_TRUNCATE":
  df.write.saveAsTable(OUTPUT_TABLE, format="delta", mode="overwrite")
