import os
import argparse
from io import BytesIO
from urllib.request import Request, urlopen

import pandas as pd
import numpy as np
import cv2
import jsonlines
from google.cloud import bigquery as bq



parser = argparse.ArgumentParser()
parser.add_argument("--pid_out", type=str, required=True)
parser.add_argument("--main_out", type=str, required=True)
args = parser.parse_args()

INPUT_SHAPE = (200, 200, 3)

IMAGE_OUTPUT_PATH = args.main_out
PRODUCT_IDS_OUTPUT_PATH = args.pid_out

USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
HEADERS = {'User-Agent':USER_AGENT,}

def _download_img(img_url: str, run_request: bool=False) -> np.array:
    
    if run_request:
        request = Request(img_url, None, HEADERS) 
        image_io = BytesIO(urlopen(request).read())
    else:
        image_io = BytesIO(urlopen(img_url).read())

    file_bytes = np.asarray(bytearray(image_io.read()), dtype=np.uint8)
    img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)

    # Fit keras Load image
    img = img[:, :, ::-1]
    return img

def _load_image(img_url):
    valid = False
    try:
        image = _download_img(img_url)
        image = cv2.resize(image, INPUT_SHAPE[:2])
        valid = True
    except Exception:
        try:
            image = _download_img(img_url, run_request=True)
            image = cv2.resize(image, INPUT_SHAPE[:2])
            valid = True
        except Exception as e:
            print(e)
            image = np.zeros(INPUT_SHAPE)
            valid = False
    return image, valid

def _main():
    c = bq.Client("fleek-prod")
    data = c.query("""SELECT DISTINCT(product_id), image_url
            FROM  `fleek-prod.gcs_exports.sagemaker_embedder_product_info`""").result().to_dataframe()
    print("Loaded input data")

    img_urls = data["image_url"].values
    product_ids = data["product_id"].tolist()
    img_file = open(IMAGE_OUTPUT_PATH, 'w+')
    img_file_writer = jsonlines.Writer(img_file)
    id_file = open(PRODUCT_IDS_OUTPUT_PATH, "w+")
    id_file_writer = jsonlines.Writer(id_file)

    n_valid = 0
    for url, product_id in zip(img_urls, product_ids):
        image, valid = _load_image(url)
        if valid:
            n_valid += 1
            data =  { "images": image.tolist()} 
            img_file_writer.write(data)
            id_file_writer.write(product_id)
            if n_valid % 250 == 0:
                print(f"Saved image {n_valid}")
    print("Great Success!")

if __name__ == "__main__":
    _main()
