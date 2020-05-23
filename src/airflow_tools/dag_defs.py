DATA_DOWNLOAD_ETL = "data_download_etl"
PRODUCT_RECOMMENDATIONS = "product_recommendations"
STAGING_MIGRATION = "daily_staging_migration"
TABLE_SETUP = "table_setup"

DAGS = [
    TABLE_SETUP,
    DATA_DOWNLOAD_ETL,
    STAGING_MIGRATION,
    PRODUCT_RECOMMENDATIONS,
]
