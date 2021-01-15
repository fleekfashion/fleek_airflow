DROP TABLE IF EXISTS {{ params.table }};
CREATE TABLE IF NOT EXISTS {{ params.table }}
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "{{ params.url }}",
  dbtable "{{ params.dbtable }}",
  user "{{ params.user }}",
  password "{{ params.password }}",
  rewriteBatchedStatements true,
  batchSize 50000
)
