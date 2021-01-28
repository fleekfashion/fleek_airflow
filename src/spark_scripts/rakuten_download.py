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
from pyspark.sql.types import StructType

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

## Remove rakuten xml files in current directory
def clear_xml_files():
    for file in os.listdir():
        if file.endswith("_mp.xml"):
            os.remove(file)

## Download rakuten xml files into current directory
def download_xml_files():
    # Connect to ftp server
    ftp = FTP()
    ftp.connect('aftp.linksynergy.com',21)
    ftp.login('Fleek','Zhu89@rgn')

    # Download + unzip xml data from Rakuten
    for filename in ftp.nlst('*_mp.xml.gz'):
        with open(filename, 'wb') as fhandle:
            print('Getting ' + filename)
            ftp.retrbinary('RETR ' + filename, fhandle.write)

        with gzip.open(filename, 'rb') as f_zip:
            with open(filename.replace(".gz", ""), 'wb') as f_unzip:
                os.remove(filename)
                shutil.copyfileobj(f_zip, f_unzip)

# Lightweight method to pull advertiser name without needing to parse xml
def get_advertiser_name(file):
    with open(file) as handle:
        header = handle.readline()
    start = header.find('<merchantName>') + len('<merchantName>')
    end = header.find('</merchantName>')
    advertiser_name = header[start:end]
    return advertiser_name

## Extract initial rakuten df from xml file
def get_rakuten_df(file):
    # Make sure advertiser is valid
    advertiser_name = get_advertiser_name(file)
    if advertiser_name not in VALID_ADVERTISERS.keys():
        return None
    advertiser_name = VALID_ADVERTISERS[advertiser_name]

    # Parse xml, check to see if advertiser name is valid
    with open(file) as handle:
        res = handle.read()

    # Parse xml file and flatten data into pandas dataframe
    data = xmltodict.parse(res)
    rakuten_df = pd.json_normalize(data['merchandiser']['product'])
    rakuten_df['advertiser_name'] = advertiser_name
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

def upload_df(df):
    schema = sqlContext.table(OUTPUT_TABLE).schema
    for name in schema.fieldNames():
        if name not in df.columns:
            df[name] = None
    schema = StructType(
        sorted(schema.fields, key=lambda x: df.columns.to_list().index(x.name))
    )
    spark_df = sqlContext.createDataFrame(data=df, schema=schema)
    spark_df.write.saveAsTable(OUTPUT_TABLE,
                        mode="append",
                        format="delta")

if __name__ == "__main__":
    clear_xml_files()
    download_xml_files()
    pd.set_option('display.max_columns', None)
    files = list(filter(lambda x: "xml" in x, os.listdir()))
    dataframes = []
    for file in files:
        rakuten_df = get_rakuten_df(file)
        if rakuten_df is not None:
            df = build_products_df(rakuten_df)
            dataframes.append(df)
    final_df = pd.concat(dataframes).reset_index(drop=True)
    upload_df(final_df)
