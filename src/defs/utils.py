from __future__ import annotations
import abc
import os
import typing as t
from functools import partial 
from dataclasses import dataclass

from functional import seq
from pyspark.sql.types import StructType
from airflow.contrib.hooks.gcp_sql_hook import CloudSQLHook


PROJECT = os.environ.get("PROJECT", "staging")
PROJECT = PROJECT if PROJECT == "prod" else "staging"

DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'


class TableDef:
    project = PROJECT

    @abc.abstractmethod
    def get_columns(self) -> seq:
        pass

    @abc.abstractmethod
    def get_full_name(self) -> str:
        pass

class DeltaTableDef(TableDef):

    def __init__(self, table_name: str, dataset: str):
        self.schema = StructType() 
        self.table_name = table_name
        self.dataset = dataset 

    def set_schema(self, schema: StructType):
        self.schema = schema

    def get_columns(self) -> seq:
        return seq(self.schema.fieldNames())

    def get_fields(self) -> seq:
        return seq(self.schema.fields)

    def get_full_name(self):
        return f"{self.project}_{self.dataset}.{self.table_name}"

    def get_name(self) -> str:
        return self.table_name

    def __hash__(self):
        return hash(self.table_name)

def load_delta_schemas(tables: dict):
    for key, value in tables.items():
        key.set_schema(value.get("schema", StructType()))
