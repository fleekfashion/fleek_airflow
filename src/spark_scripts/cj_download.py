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
from pyspark.sql.types import StructType

## Hack for linter
try:
    sqlContext = SQLContext(1)
except:
    pass

def _build_query(company_id, website_id, limit, advertiser_id, keyword):
    query = f"""
    {{
      products(companyId: "{company_id}", partnerIds: ["{advertiser_id}"], keywords: ["{keyword}"], limit:{limit}) {{
        totalCount
        count
        resultList {{
          advertiserName
          advertiserCountry
          targetCountry
          lastUpdated
          link
          mobileLink
          salePrice {{
            amount
            currency
          }}
          price {{
            amount
            currency
          }}
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
    product_label = query_params.get('product_label')
    query = _build_query(company_id=company_id,
                         website_id=website_id,
                         limit=limit,
                         advertiser_id=advertiser_id,
                         keyword=query_params['keyword']
                         )
    try:
        res = requests.post(url=url, data=query, headers=headers)
    except Exception as e:
        print("Failure to POST:", e, company_id, website_id, advertiser_id, query_params)
        return pd.DataFrame()

    try:
        batch = json.loads(res.content.decode())
        batch = batch['data']['products']["resultList"]
        print("Success:", company_id, website_id, limit, advertiser_id, query_params)
    except Exception as e:
        print("Failure to Parse:", e, company_id, website_id, limit, advertiser_id, query_params, res.content)
        return pd.DataFrame()

    ## Add Product Tag to DF
    cj_df = json_normalize(batch)
    cj_df['product_label'] = product_label
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
    final_df['advertiser_name'] = cj_df['advertiserName']
    final_df['advertiser_country'] = cj_df['advertiserCountry']
    final_df['product_brand'] = cj_df['brand']
    final_df['product_name'] = cj_df.title
    final_df['product_description'] = cj_df.description
    final_df['product_labels'] = cj_df.product_label.apply(lambda x: [x])
    final_df['product_price'] = cj_df['price.amount'].astype(float)
    final_df['product_sale_price'] = cj_df.apply(get_sale_price, axis=1).astype(float)
    final_df['product_currency'] = cj_df['price.currency']
    final_df['product_purchase_url'] = cj_df.apply(lambda x: get_correct_link(x), axis=1)
    final_df['product_image_url'] = cj_df['imageLink']
    final_df['product_additional_image_urls'] = cj_df.additionalImageLink
    return final_df


def download_batch(query_data: dict):
    df = _get_cj_df(**query_data)
    res = _build_products_df(df)
    return res


def upload_df(df, output_table):
    schema = sqlContext.table("staging_product_catalog.daily_product_dump").schema
    for name in schema.fieldNames():
        if name not in df.columns:
            df[name] = None
    schema = StructType(
        sorted(schema.fields, key=lambda x: df.columns.to_list().index(x.name))
    )
    spark_df = sqlContext.createDataFrame(data=df, schema=schema)
    spark_df.write.saveAsTable(output_table,
                        mode="append",
                        format="delta")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=str, required=True)
    args = parser.parse_args()

    with open(args.json, "rb") as handle:
        json_args = json.load(handle)
    params = json_args["params"]
    output_table = json_args["output_table"]

    dataframes = []
    for query_param in params["query_params"]:
        batch_params = copy.copy(params)
        batch_params["query_params"] = query_param
        df = download_batch(batch_params)
        dataframes.append(df)
    final_df = pd.concat(dataframes).reset_index(drop=True)
    upload_df(final_df, output_table)

        

