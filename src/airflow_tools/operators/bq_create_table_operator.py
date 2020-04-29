from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class BigQueryCreateTableOperator(BigQueryCreateEmptyTableOperator):
    """
    A custom BigQueryCreateEmptyTableOperator operator that checks if
    the table exist before creating it.
    """

    def execute(self, context):
        bq_hook = self._get_bq_hook()

        table_exists = bq_hook.table_exists(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
        )

        if table_exists:
            return

        return super().execute(context)

    def _get_bq_hook(self):
        return BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
        )
