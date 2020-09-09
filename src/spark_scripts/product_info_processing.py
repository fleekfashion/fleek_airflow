import argparse
import json
import copy

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, DataType
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

SRC_TABLE  = json_args["src_table"]
OUTPUT_TABLE = json_args["output_table"]
DS = json_args["ds"]
TIMESTAMP = json_args["timestamp"]
DROP_KWARGS_PATH= json_args["drop_kwargs_path"]

with open(DROP_KWARGS_PATH, 'r') as handle:
    drop_args = json.load(handle)


def build_filter(args):
    EXCLUDE = args.get("EXCLUDE")
    if EXCLUDE:
        args.pop("EXCLUDE")  
        exclude = _build_arg_filter(EXCLUDE)
    do_filter = _build_arg_filter(args)
    if EXCLUDE:
        return f"NOT ({do_filter}) OR ({exclude})"
    else:
        return f"NOT ({do_filter})"
    
def _build_arg_filter(args):
    and_filters = []
    for cname, drop_args in args.items():
        LOCAL_FILTERS = []
        if type(drop_args) == str:
            drop_args = [drop_args]
        for drop_arg in drop_args:
            if cname == "product_labels":
                cname = f"concat_ws(' ', {cname})"
            LOCAL_FILTERS.append(f"lower({cname}) LIKE '%{drop_arg.lower()}%'")
            
        ## IF any are true in the local args, 
        ## AND the col is not null
        ## the local drop is fulfilled
        local_f = " OR ".join(LOCAL_FILTERS)
        local_f = f"({local_f}) AND {cname} IS NOT NULL"
        and_filters.append(local_f)
    
    ## If all conditions are true, drop the the row
    f = " AND ".join(map(lambda x: f"({x})", and_filters))
    return f

###################################
## Fill in column metadata
###################################
df = sqlContext.table(SRC_TABLE )
df = df.withColumn("execution_date", F.lit(DS).cast("DATE")
         ).withColumn("execution_timestamp", F.lit(TIMESTAMP).cast("BIGINT")
         ).withColumn("product_id", F.xxhash64(F.col("advertiser_name"), F.col("product_image_url"))
         ).withColumn("product_tags", F.coalesce("product_tags", F.array().cast("array<string>")))

###################################
## Filter out invalid columns
###################################
output_schema = sqlContext.table(OUTPUT_TABLE).schema
required_fields = StructType(list(filter(lambda x: not x.nullable, output_schema.fields))).fieldNames()
for field in required_fields:
    df = df.filter(F.col(field).isNotNull())

###################################
## Filter out drop keywords 
###################################
for args in drop_args:
    f = build_filter(copy.copy(args))
    df = df.filter(f)
    print(f)

###################################
## Drop duplicate products
###################################
df = df.drop_duplicates(subset=["product_id"])

###################################
## Save table
###################################
df.write.saveAsTable(OUTPUT_TABLE, format="delta", mode="append")
