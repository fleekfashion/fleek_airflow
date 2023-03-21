# fleek_airflow

This was the backend used to power fleek fashion app. The app is no longer available so I made our backend processes and code open sourced. This backend ingested 120+ million fashion products every day from 40+ partners. There are scripts for processing clothing metadata, image information, item similarity, and preprocessing for image based search. These scripts run in parellel on a databricks Spark infrastructure. Hope you find it helpful!

App description: https://apptopia.com/ios/app/1479244278/about

# Most interesting file
Get VGG19 images embeddings distributed on spark: https://github.com/fleekfashion/fleek_airflow/blob/master/src/spark_scripts/product_ml.py
Compute product similarity using tags and image embeddings: https://github.com/fleekfashion/fleek_airflow/blob/master/src/template/compute_product_similarity.sql
Generate search suggestions from metadata: https://github.com/fleekfashion/fleek_airflow/blob/master/src/template/build_search_suggestions.sql
Products_ML Airflow Definitions: https://github.com/fleekfashion/fleek_airflow/blob/master/src/subdags/spark_product_download_etl/active_products_ml.py
