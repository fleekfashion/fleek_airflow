#!/usr/bin/env python
# coding: utf-8

import os
import json
import requests
import dateutil

import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import pyhash
from google.cloud import storage, bigquery

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

def _build_product_id(advertiser_name, image_url):
    hasher = pyhash.farm_fingerprint_64()
    return hasher(advertiser_name + image_url) // 10

def _upload_to_bigquery(df, bq_output_table):
    bq_client = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    bq_client.load_table_from_dataframe(df, bq_output_table, job_config=job_config).result()

def get_gcs_file(bucket, uri):
    c = storage.Client('fleek_prod')
    blobs = c.list_blobs(bucket_or_name=bucket,
            prefix=uri)
    blob = list(blobs)[0]
    return blob.download_as_string().decode()

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
    product_tag = query_params.get('product_tag')
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
    cj_df['product_tag'] = product_tag
    cj_df.fillna(value=pd.np.nan, inplace=True)
    return cj_df

def _drop_row(row, drop_kwargs):
    cases = []
    cases.append(row.get("product_price", np.nan) is np.nan)
    cases.append(row.get("product_image_url", "nan").lower() == "nan")
    cases.append(row.get("product_purchase_url", "nan").lower() == "nan")
    cases.append(row.get("product_name", "nan").lower() == "nan")

    ## Drop row based on value
    def drop_row_kwargs(row, drop_kwargs):
        cases = []
        for w in ["woman", "women"]:
            if w in row.get("product_name").lower():
                return False
        for key, values in drop_kwargs.items():
            for value in values:
                cases.append( (value.lower() in row.get(key, "").lower()) )
        return sum(cases) > 0 
    cases.append(drop_row_kwargs(row, drop_kwargs))
    return sum(cases) > 0

def _build_products_df(cj_df, drop_kwargs):
    ## CJ filters. 
    if "targetCountry" in cj_df.columns:
        cj_df = cj_df.loc[cj_df.targetCountry.apply(lambda x: x.lower() != "ca")]
    cj_df = cj_df.reset_index(drop=True)
    if len(cj_df) == 0:
        return pd.DataFrame()

    def get_correct_link(row):
        if str(row.get('linkCode.clickUrl', "nan")).lower() != "nan":
            return row['linkCode.clickUrl']
        if str(row.get('mobileLink', "nan")).lower() != "nan": 
            return row['mobileLink']
        return row['link']

    ## Create final df to upload
    final_df = pd.DataFrame()
    final_df['advertiser_name'] = cj_df['advertiserName']
    final_df['advertiser_country'] = cj_df['advertiserCountry'].astype("str")
    final_df['product_brand'] = cj_df['brand']
    final_df['product_name'] = cj_df.title
    final_df['product_description'] = cj_df.description
    final_df['product_tag'] = cj_df.product_tag
    final_df['product_price'] = cj_df['price.amount'].astype('float')
    final_df['product_sale_price'] = cj_df.get('salePrice.amount', cj_df.get("price.amount", pd.np.nan)).astype('float')
    final_df['product_currency'] = cj_df['price.currency']
    final_df['product_purchase_url'] = cj_df.apply(lambda x: get_correct_link(x), axis=1)
    final_df['product_image_url'] = cj_df['imageLink']
    final_df['product_additional_image_urls'] = cj_df.additionalImageLink.apply(lambda x: ",_,".join(x))
    final_df['product_last_update'] = cj_df.lastUpdated.apply(lambda x: dateutil.parser.parse(x).date())
    inds = final_df.apply(lambda x: _drop_row(x, drop_kwargs), axis=1)
    final_df = final_df.loc[~inds].reset_index(drop=True)

    return final_df

def _insert_fleek_columns(df: pd.DataFrame, kwargs: dict) -> pd.DataFrame:
    ## Insert Fleek Columns
    df["execution_date"] = kwargs["execution_date"].date()
    df["execution_timestamp"] = kwargs["execution_date"].int_timestamp
    df["product_id"] = df.apply(lambda x: _build_product_id(
            advertiser_name=x.advertiser_name,
            image_url=x.product_image_url), 
        axis=1)
    df = df.drop_duplicates(subset="product_id").reset_index(drop=True)
    return df

def download_cj_data(query_data: dict, drop_kwargs: dict,
        bq_output_table: str, **kwargs):
    query_params_list = query_data.pop("query_params")
    dataframes = []
    for query_params in query_params_list:
        query_data['query_params'] = query_params
        cj_df = _get_cj_df(**query_data)
        df = _build_products_df(cj_df, drop_kwargs)
        dataframes.append(df)
    final_df = pd.concat(dataframes).reset_index(drop=True)
    final_df = _insert_fleek_columns(final_df, kwargs)
    print(final_df.columns)
    _upload_to_bigquery(final_df, bq_output_table) 
