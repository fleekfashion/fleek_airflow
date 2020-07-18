INSERT INTO {{ params.user_events_table }} 
SELECT {{params.columns}} , 
  DATE(TIMESTAMP_SECONDS(event_timestamp)) as execution_date,
  {{ execution_date.int_timestamp }} as airflow_execution_timestamp
FROM EXTERNAL_QUERY(
  "{{params.external_conn_id}}", 
  "SELECT * FROM {{params.cloud_sql_export_table}};"
)
