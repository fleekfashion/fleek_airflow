import argparse
import os
import json
import requests
import dateutil
import copy

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

def _build_query(company_id, website_id, limit, advertiser_id, keyword):
    query = f"""
    {{
      shoppingProducts(companyId: "{company_id}", partnerIds: ["{advertiser_id}"], keywords: ["{keyword}"], limit:{limit}) {{
        totalCount
        count
        resultList {{
          advertiserName
          advertiserCountry
          targetCountry
          lastUpdated
          link
          gender
          size
          color
          availability
          isDeleted
          id
          itemGroupId
          material
          sizeType
          customLabel0
          customLabel1
          customLabel2
          customLabel3
          mobileLink
          salePrice {{
            amount
            currency
          }}
          price {{
            amount
            currency
          }}
          googleProductCategory {{
            name
          }},
          title
          brand
          description
          imageLink
          additionalImageLink
          linkCode(pid: "{website_id}") {{
            clickUrl
            imageUrl
          }}
        }}
      }}
    }}
    """
    return query

def _get_cj_df(company_id, website_id, limit, advertiser_id, query_params):
    url = "https://ads.api.cj.com/query"
    headers = {"Authorization": "Bearer 692245ytkcqqyq3k155pgyr53g"}
    query = _build_query(company_id=company_id,
                         website_id=website_id,
                         limit=limit,
                         advertiser_id=advertiser_id,
                         keyword=query_params['keyword']
                         )
    res = requests.post(url=url, data=query, headers=headers)
    batch = json.loads(res.content.decode())
    batch = batch['data']['shoppingProducts']["resultList"]

    ## Add Product Tag to DF
    cj_df = json_normalize(batch)
    cj_df.fillna(value=np.nan, inplace=True)
    cj_df = cj_df.where(pd.notnull(cj_df), None)
    cj_df = cj_df.replace(to_replace={"nan": None})
    cj_df = cj_df.replace(to_replace={np.nan: None})
    return cj_df

def _build_products_df(cj_df):
    def get_correct_link(row):
        if row.get('linkCode.clickUrl', None) is not None:
            return row['linkCode.clickUrl']
        if row.get('mobileLink', None) is not  None: 
            return row['mobileLink']
        return row['link']

    def get_sale_price(row):
        output = None
        if row.get('salePrice.amount', None) is not None:
            output = row['salePrice.amount']
        else:
            output = row['price.amount']
        return float(output)

    if len(cj_df) == 0:
          return pd.DataFrame()

    ## Create final df to upload
    final_df = pd.DataFrame()
    final_df['advertiser_country'] = cj_df['advertiserCountry']
    final_df['product_brand'] = cj_df['brand']
    final_df['product_name'] = cj_df.title
    final_df['product_description'] = cj_df.description
    final_df['product_price'] = cj_df['price.amount'].astype(float)
    final_df['product_sale_price'] = cj_df.apply(get_sale_price, axis=1).astype(float)
    final_df['product_currency'] = cj_df['price.currency']
    final_df['product_purchase_url'] = cj_df.apply(lambda x: get_correct_link(x), axis=1)
    final_df['product_image_url'] = cj_df['imageLink']
    final_df['product_additional_image_urls'] = cj_df.additionalImageLink
    final_df['color'] = cj_df.color
    final_df['size'] = cj_df["size"]
    final_df['external_id'] = cj_df.id.astype(str)
    final_df['product_external_labels'] = cj_df.apply(
        lambda x: [
            x.get('googleProductCategory', ""),
            x.get('availability', ""),
            x.get('gender', "")
        ], axis=1)
    final_df['external_group_id'] = cj_df.itemGroupId
    final_df['material'] = cj_df.material
    final_df['sizeType'] = cj_df.sizeType
    final_df['external_custom_labels'] = cj_df.apply(
        lambda x: [
            x.get(f'customLabel{i}', "")
            for i in range(4)
        ], axis=1)
    return final_df


def download_batch(query_data: dict):
    df = _get_cj_df(**query_data)
    res = _build_products_df(df)
    return res


def upload_df(df, output_table):
    schema = sqlContext.table(output_table).schema

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=str, required=True)
    args = parser.parse_args()

    with open(args.json, "rb") as handle:
        json_args = json.load(handle)
    query_data = json_args["query_data"]
    output_table = json_args["output_table"]
    advertiser_name = json_args["advertiser_name"]

    dataframes = []
    for query_param in query_data["query_params"]:
        batch_params = copy.copy(query_data)
        batch_params["query_params"] = query_param
        df = download_batch(batch_params)
        dataframes.append(df)
    final_df = pd.concat(dataframes).reset_index(drop=True)
    final_df["advertiser_name"] = advertiser_name
    upload_df(final_df, output_table)
