import json
import os
import logging
import requests

import bs4
import pyhash
import pandas as pd
import numpy as np

from google.cloud import bigquery

URL_PATH = "https://product-search.api.cj.com/v2/product-search?"
ACCESS_TOKEN_BEARER = "Bearer 692245ytkcqqyq3k155pgyr53g"
CJ_TO_SCHEMA = {
    "advertiser-id": "advertiser_id",
    "advertiser-name": "advertiser_name",
    "advertiser-category": "advertiser_category",
    "catalog-id": "catalog_id",
    "currency": "currency",
    "name": "product_name",
    "description": "product_description",
    "price": "product_price",
    "sale-price": "product_sale_price",
    "image-url": "product_image_url",
    "buy-url": "product_purchase_url"
}
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

def download_cj_data(parameters: dict, bq_output_table: str, **kwargs) -> None:

    ## Parameter Parsing
    n_pages = parameters.pop("n_pages")
    p_tag = parameters.pop("product_tag")
    parameters["records-per-page"] = "50"
    
    bq_client = bigquery.Client()
    hasher = pyhash.farm_fingerprint_64()
    for page_number in range(1, n_pages+1):
        
        ## Get and load data into dataframe
        data = _get_cj_data(parameters, page_number)
        df = pd.DataFrame(data)
        df.rename(mapper=CJ_TO_SCHEMA, axis=1, inplace=True)
        df.product_price = df.product_price.astype("float64")
        df.product_sale_price = df.product_sale_price.astype("float64")
        df["execution_date"] = kwargs["execution_date"].date()
        df["execution_timestamp"] = kwargs["execution_date"].int_timestamp
        df["product_tag"] = p_tag
        df["product_id"] = df.apply(lambda x: hasher(
            x.product_name + x.advertiser_name +
            x.product_image_url
            )//10, axis=1
        )

        # Upload to bigquery
        bq_client = bigquery.Client(project=PROJECT)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        bq_client.load_table_from_dataframe(df, bq_output_table, job_config=job_config).result()
          
        

def _get_cj_data(parameters, page_number):
    ## Create URL call from parameters
    url = URL_PATH 
    for k, v in parameters.items():
        url +=  f"{k}={v}&"
    url += f"page-number={page_number}" 
    logging.info(url)

    ## Call and parse api request
    cj_response = requests.request("GET", url,
                  headers={"Authorization" : f"{ACCESS_TOKEN_BEARER}"})
    data = _parse_cj_response(cj_response)
    return data

def _parse_cj_response(cj_response):
    """Parse cj html  output into list of dicts."""
    data = []
    ids = list(CJ_TO_SCHEMA.keys())    

    content = bs4.BeautifulSoup(cj_response.content)
    products = content.find_all("product")
    for p in products:
        filtered = p.find_all(ids)
        values = {}
        for f in filtered:
            val = f.get_text()
            if val == "":
                val = np.nan
            values[f.name] = val
        data.append(values)
    return data



