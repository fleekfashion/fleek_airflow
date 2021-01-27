import argparse
import json

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SQLContext, SparkSession

from delta.tables import DeltaTable

## Hack for linter
try:
    sqlContext = SQLContext(1)
    spark = SparkSession(sqlContext)
except:
    pass

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

## Required args
TABLE = json_args["table"]
SCHEMA = StructType.fromJson(json_args["schema"])

## Optional Args
PARTITION = json_args.get("partition")
COMMENT = json_args.get("comment", " ")

def _build_field(field, strict):
    nullable = "NOT NULL" if ( not field.nullable and strict) else ""
    c = field.metadata.get("comment")
    c = c if c else ""
    c = c.replace("'", "\\'") #handle internal quotation
    comment = f"COMMENT '{c}'" if c else ""
    if type(field.dataType) == StructType:
        internal_fields = []
        for f in field.dataType.fields:
            local_nullable = "" if f.nullable else "NOT NULL"
            internal_fields.append(
                f"{f.name}: {f.dataType.simpleString()} {local_nullable} {comment}"
            )
        struct_schema = ", ".join(internal_fields)
        return f"{field.name} {field.dataType.typeName()}<{struct_schema}> {nullable}"
    else:
        return f"{field.name} {field.dataType.simpleString()} {nullable} {comment}"

def build_fields(schema, strict):
    return ",\n".join([ _build_field(field, strict) for field in schema.fields ])

##################################
## Create initial table 
##################################
def create_table(schema, table, replace=False): 
    partition = f"PARTITIONED BY ({PARTITION})" if PARTITION else ""
    fields = build_fields(schema, strict=True)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
    {fields}
    )
    USING DELTA
    {partition}
    COMMENT '{COMMENT}'
    """
    sqlContext.sql(sql)
create_table(SCHEMA, TABLE, replace=False)

##################################
## Add new columns
##################################
old_schema = sqlContext.table(TABLE).schema
overlapping_fields = StructType(
    filter(lambda x: x.name in SCHEMA.fieldNames(), old_schema.fields)
)
new_fields = list(filter(lambda x: x.name not in old_schema.fieldNames(), SCHEMA.fields))
new_schema_fields = StructType(new_fields)
new_columns = build_fields(new_schema_fields, False)

query = f"""
ALTER TABLE {TABLE} ADD COLUMNS (
  {new_columns}
  )
"""

if len(new_fields) > 0:
    spark.sql(query)

##################################
## Order fields correctly
##################################
def order_columns():
    old_schema = spark.table(TABLE).schema
    for i in range(len(SCHEMA.fields)):
        fnew = SCHEMA.fields[i]
        fold = old_schema.fields[i]
        if fnew.name != fold.name:
            if i == 0:
                spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {fnew.name} FIRST")
            else:
                spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {fnew.name} AFTER {SCHEMA.fields[i-1].name}")
            ## update schema with new ordering
            old_schema = spark.table(TABLE).schema
order_columns()

##############################################################
## Get fields to change null constraint
#############################################################
new_not_null_fields = []
new_null_fields = []
old_schema = spark.table(TABLE).schema 
for fnew, fold in zip(SCHEMA.fields, old_schema.fields):
    if fnew.nullable != fold.nullable:
        if fnew.nullable:
            new_null_fields.append(fnew)
        elif not fnew.nullable:
            new_not_null_fields.append(fnew)

##################################
## Set relevant default values
##################################
def _build_update_set(fields):
    res = dict()
    for field in fields:
        default_value = field.metadata.get("default")
        if default_value is None:
            continue
        if type(default_value) == list:
            value = F.array([F.lit(v) for v in default_value]).cast(field.dataType.simpleString())
        else:
            value = F.lit(default_value)
        res[field.name] = F.coalesce(field.name, value)
    return res

t = DeltaTable.forName(spark, TABLE)
update_set = _build_update_set(new_not_null_fields)
t.update(set=update_set)

##################################
## Add not null constraints
##################################
for field in new_not_null_fields:
    query = f"ALTER TABLE {TABLE} ALTER COLUMN {field.name} SET NOT NULL"
    spark.sql(query)

##################################
## Drop not null constraints
##################################
for field in new_null_fields:
    query = f"ALTER TABLE {TABLE} ALTER COLUMN {field.name} DROP NOT NULL"
    spark.sql(query)


##################################
## Update Comments 
##################################
for field in SCHEMA.fields:
    spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {field.name} COMMENT '{field.metadata.get('comment', ' ')}'")
