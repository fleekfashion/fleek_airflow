import argparse
import json

from pyspark.sql.types import *
from pyspark.sql.functions import lit
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
TABLE = json_args["table"]
SCHEMA = StructType.fromJson(json_args["schema"])

## Optional Args
PARTITION = json_args.get("partition")
COMMENT = json_args.get("comment")

def build_fields(schema):
    def _build_field(field):
        nullable = "" if field.nullable else "NOT NULL"
        c = field.metadata.get("comment")
        comment = f"COMMENT '{c}'" if c else ""
        if type(field.dataType) != StructType:
            return f"{field.name} {field.dataType.typeName()} {nullable} {comment}"
        else:
            internal_fields = []
            for f in field.dataType.fields:
                local_nullable = "" if f.nullable else "NOT NULL"
                internal_fields.append(
                    f"{field.name}: {field.dataType.typeName()} {local_nullable} {comment}"
                )
            internal = ",".join
            return f"{field.name} {field.dataType.typeName()}<{internal}> {nullable}"
    return ",\n".join([ _build_field(field) for field in schema.fields ])

def create_table(schema, table, replace=False): 
    table_statement= "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    partition = f"PARTITION BY ({PARTITION})" if PARTITION else ""
    comment = f"COMMENT '{COMMENT}'" if COMMENT else ""
    fields = build_fields(schema)

    sql = f"""{table_statement} {table}
    ({fields})
    USING DELTA
    {partition}
    {comment}
    """
    print(sql)
    sqlContext.sql(sql)

# Save initial table
create_table(SCHEMA, TABLE, replace=False)

old_schema = sqlContext.table(TABLE).schema
if old_schema.fieldNames() != SCHEMA.fieldNames():
    temp_table = TABLE.split(".")[-1]+"_temp"
    overlapping_fields = StructType(
        filter(lambda x: x.name in SCHEMA.fieldNames(), old_schema.fields)
    )
    new_fields = filter(lambda x: x.name not in old_schema.fieldNames(), SCHEMA.fields)

    transformed_df = sqlContext.table(TABLE).select(overlapping_fields.fieldNames())
    for field in new_fields:
        transformed_df = transformed_df.withColumn(field.name, lit(field.metadata.get("default")))
    transformed_df.registerTempTable(temp_table)

    sqlContext.sql(f"DROP TABLE IF EXISTS {TABLE}")
    create_table(SCHEMA, TABLE, replace=True)
    sqlContext.sql(f"INSERT INTO {TABLE} SELECT * FROM {temp_table}")
