DATA_DOWNLOAD_ETL = "daily_product_download_etl"
DATABRICKS_SETUP = "databricks_setup"
PRODUCT_RECOMMENDATIONS = "product_recommender_stream"
STAGING_MIGRATION = "daily_staging_migrations"
TABLE_SETUP = "table_setup"
USER_EVENTS = "user_events_stream"

DAGS = [
    TABLE_SETUP,
    DATA_DOWNLOAD_ETL,
    STAGING_MIGRATION,
    PRODUCT_RECOMMENDATIONS,
    USER_EVENTS
]
