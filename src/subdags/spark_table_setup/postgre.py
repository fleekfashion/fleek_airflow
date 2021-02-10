"""
TLDR: Set up Postgre Personalization Tables.

"""

import os
from datetime import timedelta
import typing as t

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from functional import seq

from src.airflow_tools.operators import cloudql_operators as csql
from src.airflow_tools.queries import postgre_queries as pquery 
from src.airflow_tools.databricks.databricks_operators import SparkSQLOperator
from src.defs.postgre import utils as u
from src.defs import postgre
from src.defs.delta import postgres as spark_postgre

POSTDEFS = [postgre.product_catalog, postgre.user_data, postgre.spark_personalization]

def create_table(table: u.PostgreTable) -> str:

    def _build_field(col: u.Column) -> str:
        return f"{col.name} {col.type}"

    def _add_column(col: u.Column) -> str:
        return f"ALTER TABLE {table.get_full_name()} ADD COLUMN \
                IF NOT EXISTS {_build_field(col)}"

    def _set_default_vals(col: u.Column) -> str:
        default = pquery._py_to_pg(col.default, col.type)
        return f"{col.name} = coalesce({col.name}, {default})"

    def _set_nullability(col: u.Column) -> str:
        constraint = "SET NOT NULL" if not col.nullable else "DROP NOT NULL"
        return f"ALTER TABLE {table.get_full_name()} ALTER COLUMN {col.name} {constraint}"

    def _create_index(index: u.Index) -> str:
        desc = "UNIQUE" if index.unique else ""
        return f"""CREATE {desc} INDEX IF NOT EXISTS {index.get_name(table.name)}
        ON {table.get_full_name()} ({', '.join(index.columns)} )"""

    def _create_pk_stmt(pk: t.Optional[u.PrimaryKey]) -> str:
        if pk is None:
            return ""
        else:
            return f"constraint {pk.get_name(table.name)} primary key ({', '.join(pk.columns)})"

    def _create_fk_stmt(fk: u.ForeignKey) -> str:
        return f"""
        DO $$
        BEGIN

            ALTER TABLE {table.get_full_name()} ADD CONSTRAINT {fk.get_name(table.name)}
            FOREIGN KEY ({', '.join(fk.columns)})
            REFERENCES {fk.ref_table}({', '.join(fk.ref_columns)});
        EXCEPTION
            WHEN duplicate_object THEN RAISE NOTICE 'Table constraint {fk.get_name(table.name)} already exits';
        END $$;
        """

    fields = table.get_schema() \
            .map(_build_field) \
            .make_string(",\n")
    add_fields_stmt = table.get_schema() \
            .map(_add_column) \
            .make_string(";\n") or ""
    pk_stmt = _create_pk_stmt(table.primary_key)
    create_indexes_stmt = table.indexes.map(_create_index) \
            .make_string(";\n") or ""
    create_fks_stmt = table.foreign_keys.map(_create_fk_stmt) \
            .make_string("") or ""

    default_values = table.get_schema() \
            .filter(lambda x: x.default is not None) \
            .map(lambda x: _set_default_vals(x.default)) \
            .make_string(", ")
    set_default_stmt = f"""
    UPDATE {table.get_full_name()}
        SET {default_values};
    """ if len(default_values) > 0 else ""

    nullability_stmt = table.get_schema() \
            .map(_set_nullability) \
            .make_string(";\n") or ""
    query = f"""
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS {table.get_full_name()} (
        {fields}{',' if table.primary_key is not None else ''}
        {pk_stmt}
    );
    {add_fields_stmt};
    {set_default_stmt};
    {nullability_stmt};
    {create_indexes_stmt};
    {create_fks_stmt};
    END TRANSACTION;"""

    return query


def get_operators(dag: DAG) -> dict:
    f"""
    {__doc__} 
    """
    head = DummyOperator(task_id="postgre_table_setup_head", dag=dag)
    tail = DummyOperator(task_id="postgre_table_setup_tail", dag=dag)

    for table in u.TABLES:
        prev_op = head
        for t in [table, table.get_staging_table()]:
            op = postgre_build_product_table = CloudSqlQueryOperator(
                dag=dag,
                gcp_cloudsql_conn_id=u.CONN_ID,
                task_id=f"create_postgres_{t.name}_table",
                sql=create_table(
                    table=t
                )
            )
            prev_op >> op
            prev_op = op
        
        for t in [table, table.get_staging_table()]:
            op = SparkSQLOperator(
                        task_id=f"create_postgres_{t.name}_delta_hook",
                        dag=dag,
                        params={
                            "table": t.get_delta_name(),
                            "url": os.environ["SPARK_CLOUD_SQL_URL"],
                            "dbtable": f"{t.get_full_name()}",
                            "user": os.environ["CLOUD_SQL_USER"],
                            "password": os.environ["CLOUD_SQL_PASSWORD"]
                        },
                        sql="template/jdbc_delta_hook.sql",
                        local=True
                    )
            prev_op >> op >> tail
    return {"head": head, "tail": tail}
