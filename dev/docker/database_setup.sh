#!/usr/bin/env bash

echo "running postgres setup"

fernet_encrypt() {
  python -c "from cryptography.fernet import Fernet; import sys; f = Fernet('$AIRFLOW_FERNET_KEY'); print f.encrypt(sys.argv[1])" "$@"
}

PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -d $POSTGRES_DB -U $POSTGRES_USER << EOF
--Delete default connections Airflow provides
DELETE FROM connection;

--Add GCP connections
INSERT INTO connection(conn_id, conn_type, extra)
	SELECT 'bigquery_default', 'google_cloud_platform', '{"extra__google_cloud_platform__project": "$GOOGLE_CLOUD_PROJECT",
	                                                      "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='bigquery_default'
	);

INSERT INTO connection(conn_id, conn_type, extra)
	SELECT 'google_cloud_default', 'google_cloud_platform', '{"extra__google_cloud_platform__project": "$GOOGLE_CLOUD_PROJECT",
	                                                          "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='google_cloud_default'
	);

INSERT INTO connection(conn_id, conn_type, extra)
	SELECT 'google_cloud_datastore_default', 'google_cloud_platform', '{"extra__google_cloud_platform__project": "$GOOGLE_CLOUD_PROJECT",
	                                                                    "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='google_cloud_datastore_default'
	);

INSERT INTO connection(conn_id, conn_type, extra)
	SELECT 'google_cloud_storage_default', 'google_cloud_platform', '{"extra__google_cloud_platform__project": "$GOOGLE_CLOUD_PROJECT",
                                                                      "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='google_cloud_storage_default'
	);

INSERT INTO connection(conn_id, conn_type, extra)
	SELECT 'google_cloud_storage_default', 'google_cloud_platform', '{"extra__google_cloud_platform__project": "$GOOGLE_CLOUD_PROJECT",
                                                                      "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='google_cloud_storage_default'
	);

INSERT INTO connection(conn_id, conn_type, extra)
  SELECT 'aws_default', 'aws', '{"s3_config_file": $AWS_SHARED_CREDENTIALS_FILE, "s3_config_format": "aws", "region_name": "us-east-2"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='aws_default'
	);

INSERT INTO connection(conn_id, conn_type, extra)
  SELECT 's3_default', 's3', '{"s3_config_file": $AWS_SHARED_CREDENTIALS_FILE, "s3_config_format": "aws", "region_name": "us-east-2"}'
	WHERE NOT EXISTS (
		SELECT 1 FROM connection WHERE conn_id='s3_default'
	);
EOF

echo "ran postgres setup"
