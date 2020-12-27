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
FORMAT = json_args.get("format") or "delta"
DROP_DUPLICATES = json_args.get("drop_duplicates") or False
SUBSET = json_args.get("duplicates_subset")

def process_df(df):
    if DROP_DUPLICATES:
        df = df.drop_duplicates(subset=SUBSET)
    return df

# Run SQL
for query in SQL.split(";"):
    df = sqlContext.sql(query)

processed_df = df.transform(
                lambda df: df.drop_duplicates(subset=SUBSET)
                if DROP_DUPLICATES else df)

if MODE == "WRITE_APPEND":
    processed_df.write.saveAsTable(OUTPUT_TABLE, format=FORMAT, mode="append")
elif MODE == "WRITE_TRUNCATE":
    processed_df.write.saveAsTable(OUTPUT_TABLE, format=FORMAT, mode="overwrite")
