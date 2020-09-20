import urllib.request
import shutil# Download URL and save to outpath.
import os
import pathlib
import argparse
import json

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

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
print(json_args)

DS = json_args["ds"]
OUTPATH = json_args["outpath"]
SRC_TABLE = json_args["src_table"]
ACTIVE_PRODUCTS_TABLE = json_args["active_products_table"]

try:
    shutil.rmtree(OUTPATH)
except:
    pass
pathlib.Path(OUTPATH).mkdir(parents=True, exist_ok=True)

def downloader(url, pid, outdir):
    # From URL construct the destination path and filename.
    outpath = f"{outdir}/{pid}.jpg"
    if os.path.exists(outpath):
        return True
    try:
        urllib.request.urlretrieve(url, outpath)
    except:
        return False
    return True

sql = f"""
SELECT product_image_url, product_id 
FROM {SRC_TABLE}
WHERE execution_date='{DS}'
AND product_id NOT IN (
    SELECT product_id FROM {ACTIVE_PRODUCTS_TABLE}
)
"""
print(sql)

downloadUDF = F.udf(downloader, BooleanType())
res_df = sqlContext.sql(sql
    ).filter(downloadUDF(
        F.col("product_image_url"),
        F.abs(F.col("product_id")),
        F.lit(OUTPATH)
        )
    )
res_df.foreach(lambda x: None)
