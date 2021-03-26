import argparse
import json
from ftplib import FTP
import gzip
import shutil
import os

import xml.etree.ElementTree as ET
import xmltodict
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from pyspark.sql import SQLContext
from pyspark.sql.types import *

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
ADID = json_args['adid']
ADVERTISER_NAME = json_args['advertiser_name']
OUTPUT_TABLE = json_args['output_table']

## Remove rakuten xml files in current directory
def clear_xml_files():
    for f in os.listdir():
        if f.endswith("_mp.xml") and str(ADID) in f:
            os.remove(f)

## Download rakuten xml files into current directory
def download_xml_files():
    # Connect to ftp server
    ftp = FTP()
    ftp.connect('aftp.linksynergy.com',21)
    ftp.login('Fleek','Zhu89@rgn')

    # Download + unzip xml data from Rakuten
    for filename in ftp.nlst(f'{ADID}*_mp.xml.gz'):
        with open(filename, 'wb') as fhandle:
            print('Getting ' + filename)
            ftp.retrbinary('RETR ' + filename, fhandle.write)

        with gzip.open(filename, 'rb') as f_zip:
            with open(filename.replace(".gz", ""), 'wb') as f_unzip:
                os.remove(filename)
                shutil.copyfileobj(f_zip, f_unzip)
    print(f"\nDONE {ADVERTISER_NAME}")

## Extract initial rakuten df from xml file
def get_rakuten_df(file):
    # Parse xml, check to see if advertiser name is valid
    with open(file, 'r') as handle:
        res = handle.read()

    # Parse xml file and flatten data into pandas dataframe
    print(f"\nStart xml parse {ADVERTISER_NAME}")
    data = xmltodict.parse(res)
    print(f"\nFinish xml parse {ADVERTISER_NAME}")
    rakuten_df = pd.json_normalize(data['merchandiser']['product'])
    print(f"\nFinish json parse {ADVERTISER_NAME}")
    rakuten_df['advertiser_name'] = ADVERTISER_NAME
    if 'category.secondary' not in rakuten_df.columns: 
        rakuten_df['category.secondary'] = None

    # Replace all null values with None values
    rakuten_df.fillna(value=np.nan, inplace=True)
    rakuten_df = rakuten_df.where(pd.notnull(rakuten_df), None)
    rakuten_df = rakuten_df.replace(to_replace={"nan": None})
    rakuten_df = rakuten_df.replace(to_replace={np.nan: None})
    return rakuten_df

## Build final products df 
def build_products_df(rakuten_df):
    # Construct array of product labels from primary and secondary categories (if they exist)
    def _get_product_labels(row):
        product_labels = [
            row.get('category.primary', ''),
            row.get('category.secondary', ''),
            row.get('attributeClass.Gender', ''),
            row.get('shipping.availability', '')
        ]
        return product_labels

    # Case where only sale price listed
    def _get_price(row):
        output = None
        if row.get('price.retail') is not None:
            output = row['price.retail']
        else:
            output = row['price.sale']
        return float(output)

    # Case where only retail price listed
    def _get_sale_price(row):
        output = None
        if row.get('price.sale', None) is not None:
            output = row['price.sale']
        else:
            output = row['price.retail']
        return float(output)

    # Pull image urls if available
    def _get_additional_image_urls(row):
        image_urls = []
        if row.get('m1', None) is not None:
            image_urls = [row['m1']]
        return image_urls

    # Convert to final pandas dataframe
    final_df = pd.DataFrame()
    final_df['advertiser_name'] = rakuten_df['advertiser_name']
    final_df['product_brand'] = rakuten_df['brand']
    final_df['product_name'] = rakuten_df['@name']
    final_df['product_description'] = rakuten_df['description.long']
    final_df['product_external_labels'] = rakuten_df.apply(_get_product_labels, axis=1)
    final_df['product_price'] = rakuten_df.apply(_get_price, axis=1).astype(float)
    final_df['product_sale_price'] = rakuten_df.apply(_get_sale_price, axis=1).astype(float)
    final_df['product_currency'] = rakuten_df['price.@currency']
    final_df['product_purchase_url'] = rakuten_df['URL.product']
    final_df['product_image_url'] = rakuten_df['URL.productImage']
    final_df['product_additional_image_urls'] = rakuten_df.apply(_get_additional_image_urls, axis=1)
    final_df['color'] = rakuten_df.get('attributeClass.Color')
    final_df['size'] = rakuten_df.get('attributeClass.Size')
    return final_df

def upload_df(df, output_table):
    schema = sqlContext.table("staging_product_catalog.daily_product_dump").schema

    ## Add unlisted fields to the schema
    for name in df.columns:
        if name not in schema.fieldNames():
            schema.add(StructField(name, StringType()))

    for name in schema.fieldNames():
        if name not in df.columns:
            df[name] = None

    ordered_schema= StructType(
        sorted(schema.fields, key=lambda x: df.columns.to_list().index(x.name))
    )
    spark_df = sqlContext.createDataFrame(data=df, schema=ordered_schema)
    spark_df.write.option("mergeSchema", "true").saveAsTable(output_table,
                        mode="append",
                        format="delta")

if __name__ == "__main__":
    clear_xml_files()
    download_xml_files()
    files = list(filter(lambda x: str(ADID) in x, os.listdir())) ## only read relevent files
    dataframes = []
    for f in files:
        rakuten_df = get_rakuten_df(f)
        if rakuten_df is not None:
            df = build_products_df(rakuten_df)
            dataframes.append(df)
    final_df = pd.concat(dataframes).reset_index(drop=True)
    upload_df(final_df, OUTPUT_TABLE)
