import argparse
import json
import requests
import gzip
import io

import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StringType
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from functional import seq
import pandas as pd

## Hack for linter
try:
    sqlContext: SQLContext = SQLContext(1)
    sc: SparkContext = SparkContext()
    spark: SparkSession = SparkSession(sc)
except Exception:
    pass

parser = argparse.ArgumentParser()
parser.add_argument("--json", type=str, required=True)
args = parser.parse_args()
with open(args.json, "rb") as handle:
    json_args = json.load(handle)

OUTPUT_TABLE = json_args['output_table']
ADVERTISERS = json_args['advertisers']
PARAMS = json_args['params']
SQL = json_args['sql']

res = [ {"merchant_name": key, "advertiser_name": value} 
        for key, value in ADVERTISERS.items() ]
sqlContext.read.json(sc.parallelize(res)) \
        .createOrReplaceTempView(PARAMS['advertiser_name_map_table'])

URL = "https://productdata.awin.com/datafeed/download/apikey/6bc1f0112168756a643bde4d8b3d5ee4/language/en/cid/97,98,142,129,595,539,147,149,626,135,163,168,159,169,161,170,137,171,548,174,178,179,175,172,623,139,614,189,194,141,205,198,206,203,208,199,204,201/fid/14867,14929,24933,30229,52319/columns/aw_deep_link,product_name,aw_product_id,merchant_product_id,merchant_image_url,description,merchant_category,search_price,merchant_name,merchant_id,category_name,category_id,aw_image_url,currency,store_price,delivery_cost,merchant_deep_link,language,last_updated,display_price,data_feed_id,brand_name,colour,specifications,product_model,dimensions,promotional_text,brand_id,product_short_description,model_number,keywords,product_type,merchant_thumb_url,in_stock,stock_quantity,valid_from,valid_to,is_for_sale,web_offer,pre_order,stock_status,size_stock_status,size_stock_amount,rrp_price,saving,savings_percent,base_price,base_price_amount,base_price_text,product_price_old,commission_group,merchant_product_category_path,merchant_product_second_category,merchant_product_third_category,custom_1,custom_2,custom_3,custom_4,custom_5,custom_6,custom_7,custom_8,custom_9,Fashion%3Apattern,Fashion%3Aswatch,Fashion%3Asuitable_for,Fashion%3Acategory,Fashion%3Asize,Fashion%3Amaterial,condition,large_image,alternate_image,aw_thumb_url,alternate_image_two,alternate_image_three,alternate_image_four/format/csv/delimiter/%2C/compression/gzip/adultcontent/1/"

## Download Data
res = requests.get(URL) # type: ignore
decompressed = gzip.decompress(res.content) # type: ignore
df: pd.DataFrame = pd.read_csv(io.BytesIO(decompressed))

## Process data
parsed_df = df.where(pd.notnull(df), '')
for c in parsed_df.columns:
  parsed_df[c] = parsed_df[c].map(lambda x: str(x))
spark_df = sqlContext.createDataFrame(data=parsed_df) \
    .createOrReplaceTempView(PARAMS['new_products_table'])

## SQL processing
res_df = spark.sql(SQL)

## Fit into delta schema
schema = sqlContext.table(OUTPUT_TABLE).schema
for name in res_df.columns:
    if name not in schema.fieldNames():
        schema.add(StructField(name, StringType()))
for name in schema.fieldNames():
    if name not in res_df.columns:
        res_df = res_df.withColumn(name, F.lit(None))
res_df.write.option("mergeSchema", "true") \
        .saveAsTable(
                OUTPUT_TABLE,
                mode="append",
                format="delta"
        )
