DATA_DOWNLOAD_ETL = "data_download_etl"
PRODUCT_RECOMMENDATIONS = "product_recommendations"
STAGING_MIGRATION = "daily_staging_migration"
TABLE_SETUP = "table_setup"
USER_EVENTS = "user_events"

DAGS = [
    TABLE_SETUP,
    DATA_DOWNLOAD_ETL,
    STAGING_MIGRATION,
    PRODUCT_RECOMMENDATIONS,
    USER_EVENTS
]
