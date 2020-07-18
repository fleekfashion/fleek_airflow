DATA_DOWNLOAD_ETL = "product_download_etls"
PRODUCT_RECOMMENDATIONS = "product_recommenders_stream"
STAGING_MIGRATION = "daily_staging_migrations"
TABLE_SETUP = "table_setups"
USER_EVENTS = "user_event_stream"

DAGS = [
    TABLE_SETUP,
    DATA_DOWNLOAD_ETL,
    STAGING_MIGRATION,
    PRODUCT_RECOMMENDATIONS,
    USER_EVENTS
]
