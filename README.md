# fleek_airflow

This was the backend used to power fleek fashion app. The app is no longer available so I made our backend processes and code open sourced. This backend ingested 120+ million fashion products every day from 40+ partners. There are scripts for processing clothing metadata, image information, item similarity, and preprocessing for image based search. These scripts run in parellel on a databricks Spark infrastructure. Hope you find it helpful!


App description: https://apptopia.com/ios/app/1479244278/about


## Set up Instructions

1. Download gcloud-sdk
2. glcoud init
3. gcloud auth application-default login
