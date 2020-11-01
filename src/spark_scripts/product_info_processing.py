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
LABELS_PATH = json_args["labels_path"]

with open(DROP_KWARGS_PATH, 'r') as handle:
    drop_args = json.load(handle)
with open(LABELS_PATH, 'r') as handle:
    LABEL_DEFS = json.load(handle)

def load_df():
    df = sqlContext.table(SRC_TABLE)
    return df.withColumn("execution_date", F.lit(DS).cast("DATE")) \
             .withColumn("execution_timestamp", F.lit(TIMESTAMP).cast("BIGINT")) \
             .withColumn("product_id", F.abs(F.xxhash64(F.col("advertiser_name"), F.col("product_image_url")))) \
             .withColumn("product_tags",
                     F.coalesce("product_tags", F.array().cast("array<string>"))) \
             .withColumn("product_labels",
                     F.coalesce("product_labels", F.array().cast("array<string>"))) \
             .drop_duplicates(subset=["product_id"])

def _build_arg_filter(args):
    and_filters = []
    for cname, drop_args in args.items():
        LOCAL_FILTERS = []
        if type(drop_args) == str:
            drop_args = [drop_args]
        for drop_arg in drop_args:
            if cname == "product_labels":
                cname = f"concat_ws(' ', {cname})"
            drop_arg = drop_arg.lower().replace('\\', '\\\\')
            LOCAL_FILTERS.append(f"lower({cname}) rLIKE '{drop_arg}'")
            
        ## IF any are true in the local args, 
        ## AND the col is not null
        ## the local drop is fulfilled
        local_f = " OR ".join(LOCAL_FILTERS)
        local_f = f"({local_f}) AND {cname} IS NOT NULL"
        and_filters.append(local_f)
    
    ## If all conditions are true, drop the the row
    f = " AND ".join(map(lambda x: f"({x})", and_filters))
    return f

def _build_label_filter(label_def):
    conditions = []
    for d in label_def:
        conditions.append(_build_arg_filter(d))
    return f"({' OR '.join(conditions)})"

def apply_labels(df):
    for label, values in LABEL_DEFS.items():
        f = _build_label_filter(values)
        df = df.withColumn("product_labels", 
              F.when(F.expr(f), 
                     F.array_union(
                       F.col('product_labels'),
                       F.array(F.lit(label)))
                    ).otherwise(F.col('product_labels'))
        )
        print(f"Apply Label Filter: {f}")
    return df
  
def _build_drop_args_filter(args):
    EXCLUDE = args.get("EXCLUDE")
    if EXCLUDE:
        args.pop("EXCLUDE")  
        exclude = _build_arg_filter(EXCLUDE)
    do_filter = _build_arg_filter(args)
    if EXCLUDE:
        return f"NOT ({do_filter}) OR ({exclude})"
    else:
        return f"NOT ({do_filter})"

def drop_rows(df):
    ## Drop rows with invalid null fields
    output_schema = sqlContext.table(OUTPUT_TABLE).schema
    required_fields = StructType(list(filter(lambda x: not x.nullable, output_schema.fields))).fieldNames()
    for field in required_fields:
        df = df.filter(F.col(field).isNotNull())

    ## Drop rows based on drop args
    for args in drop_args:
        f = _build_drop_args_filter(copy.copy(args))
        df = df.filter(f)
        print(f"Drop Args Filter: {f}")

    ## Drop rows with no labels
    df = df.where(F.size('product_labels') > 0)
    return df

######################################################################
## Apply processing 
######################################################################
df = load_df()
df = apply_labels(df)
df = drop_rows(df)
df = df.withColumn('product_additional_image_urls', F.expr("array_remove(product_additional_image_urls, product_image_url)") )

###################################
## Save table
###################################
df.write.saveAsTable(OUTPUT_TABLE, format="delta", mode="append")
