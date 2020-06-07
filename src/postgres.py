"""
TLDR: Set up BigQuery Tables for All Dags.

Overview
1. Delete all temporary tables
2. Delete all import/export tables
3. Create tables in schema files
"""

from datetime import timedelta

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator 
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator

from src.airflow_tools.airflow_variables import DEFAULT_DAG_ARGS
from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.queries import postgre_queries as pquery
from src.defs.bq import gcs_imports, gcs_exports, personalization as pdefs
from src.defs.postgre import utils as postutils
from src.defs.postgre import personalization as postdefs
from src.subdags import table_setup

DAG_ID = "testing"
dag = DAG(
        DAG_ID,
        schedule_interval=timedelta(days=1),
        default_args=DEFAULT_DAG_ARGS,
        doc_md=__doc__
    )

################################
## User Recs 
################################
TABLE_NAME = gcs_imports.get_full_name(gcs_imports.USER_PRODUCT_RECOMMENDATIONS_TABLE)
DEST = gcs_exports.get_full_name(gcs_exports.USER_RECOMMENDATIONS_TABLE)
TOP_N = 10
BATCH_SIZE = 3
BATCHES = [ b for b in range(0, TOP_N, BATCH_SIZE)]
SQL = """
SELECT 
     user_id,
      {{ params.batch }} as batch{% for i, ind in params.inds %}, top_product_ids_{{ind}} as top_products_{{i}}{% endfor %}
FROM
  `{{ params.rec_table}}`
  """


del_rec_table = BigQueryTableDeleteOperator(
    task_id=f"delete_rec_table",
    dag=dag,
    deletion_dataset_table=DEST,
    ignore_if_missing=True,
)

bq_rec_export_head = DummyOperator(task_id="bq_rec_export_head", dag=dag)
bq_rec_export_tail = DummyOperator(task_id="bq_rec_export_tail", dag=dag)

for i in range(1, len(BATCHES)):
    BATCH = i
    inds = list(enumerate(range(BATCHES[i-1], BATCHES[i])))
    parameters = {
        "batch": BATCH,
        "inds": inds,
        "rec_table": TABLE_NAME
    }

    bq_op = BigQueryOperator(
        dag=dag,
        task_id=f"bq_op_test_{i}",
        sql=SQL,
        params=parameters,
        destination_dataset_table=DEST,
        write_disposition="WRITE_APPEND",
        use_legacy_sql=False,
    )
    bq_rec_export_head >> bq_op >> bq_rec_export_tail

GCS_DEST = "gs://fleek-prod/personalization/postgre_upload/product_recs"
recs_bq_to_gcs = BigQueryToCloudStorageOperator(
    dag=dag,
    task_id="recs_bq_to_gcs",
    source_project_dataset_table=DEST,
    destination_cloud_storage_uris=GCS_DEST,
    export_format="CSV",
    print_header=False
)

TABLE = postdefs.USER_RECOMMENDATIONS_TABLE
STAGING_TABLE = TABLE + postutils.DENOMER
columns = [
    "user_id bigint NOT NULL",
    "batch integer NOT NULL"
]
for b in range(BATCH_SIZE):
    columns.append(f"top_products_{b} bigint NOT NULL")

tail = f"""PARTITION BY LIST(batch);
CREATE INDEX ON user_product_recs (user_id);
"""
for b in range(len(BATCHES)):
    tail += f"CREATE TABLE IF NOT EXISTS {STAGING_TABLE}_{b} PARTITION OF {STAGING_TABLE} FOR VALUES IN ({b});\n"

postgre_build_rec_table = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="build_postgres_user_product_recs_table",
    sql=pquery.create_table_query(
        STAGING_TABLE,
        columns,
        tail=tail,
        drop=True
    ),
)
    
data_import = csql.get_import_operator(
        dag=dag,
        task_id="postgre_import_rec_data",
        uri=GCS_DEST,
        database="ktest",
        table=STAGING_TABLE,
        instance=postdefs.INSTANCE
    )

tail = ""
for b in range(len(BATCHES)):
    tail += f"ALTER TABLE {STAGING_TABLE}_{b} RENAME TO {TABLE}_{b};\n"

postgre_rec_table_staging_to_prod = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="postgres_user_product_recs_staging_to_prod",
    sql=pquery.staging_to_live_query(
        staging_name=STAGING_TABLE,
        table_name=TABLE,
        mode="OVERWRITE",
        tail=tail
    ),
)

################################
## PRODUCT TABLE
################################
TABLE_NAME = pdefs.FULL_NAMES[pdefs.ACTIVE_PRODUCTS_TABLE]
DEST = "fleek-prod.gcs_exports.postgre_product_info"

cols = []
for c in postdefs.SCHEMAS[postdefs.PRODUCT_INFO_TABLE]["schema"]:
    cols.append(c)

last_col = cols[-1]
parameters = {
    "prod_table": TABLE_NAME,
    "cols": cols[:-1],
    "last_col": last_col
}


SQL = """
SELECT {% for col in params.cols %} 
    {{col}}, {% endfor %}
    {{ params.last_col }}
FROM {{ params.prod_table }}
"""

prod_info_bq_export = BigQueryOperator(
    dag=dag,
    task_id=f"prod_info_to_gcs_exports",
    sql=SQL,
    params=parameters,
    destination_dataset_table=DEST,
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
)


POSTGRE_PTABLE = "product_info"
GCS_DEST = "gs://fleek-prod/personalization/postgre_upload/product_info"
prods_bq_to_gcs = BigQueryToCloudStorageOperator(
    dag=dag,
    task_id="prod_bq_to_gcs",
    source_project_dataset_table=DEST,
    destination_cloud_storage_uris=GCS_DEST,
    export_format="CSV",
    print_header=False

)

staging_name = POSTGRE_PTABLE + "_staging"
postgre_build_product_staging_table = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="build_postgres_product_info_staging_table",
    sql=pquery.create_staging_table_query(
        table_name=POSTGRE_PTABLE,
        denomer="_staging"
    )
)

product_data_import = csql.get_import_operator(
    dag=dag,
    task_id="postgre_import_product_data",
    uri=GCS_DEST,
    database="ktest",
    table=staging_name,
    instance=postdefs.INSTANCE,
    columns=cols,
)

product_info_staging_to_prod = CloudSqlQueryOperator(
    dag=dag,
    gcp_cloudsql_conn_id=postdefs.CONN_ID,
    task_id="postgres_product_info_staging_to_live",
    sql=pquery.staging_to_live_query(
        table_name=POSTGRE_PTABLE,
        staging_name=staging_name,
        mode="UPDATE_APPEND",
        key="product_id",
    )
)

del_rec_table >> bq_rec_export_head
bq_rec_export_tail >> recs_bq_to_gcs >> postgre_build_rec_table
postgre_build_rec_table >> data_import >> postgre_rec_table_staging_to_prod >> prod_info_bq_export

prod_info_bq_export >> prods_bq_to_gcs
prods_bq_to_gcs >> postgre_build_product_staging_table >> product_data_import >> product_info_staging_to_prod

