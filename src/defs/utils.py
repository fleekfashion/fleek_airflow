from __future__ import annotations
import abc
import os
import typing as t
from functools import partial 
from dataclasses import dataclass

from functional import seq
from pyspark.sql.types import StructType
from airflow.contrib.hooks.gcp_sql_hook import CloudSQLHook
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base, name_for_collection_relationship

PROJECT = os.environ.get("PROJECT", "staging")
PROJECT = PROJECT if PROJECT == "prod" else "staging"

DATABASE = "ktest"
CONN_ID = f'google_cloud_sql_{DATABASE}'
DATABASE_USER = "postgres"
PASSWORD = os.environ["CLOUD_SQL_PASSWORD"]

conn_str = f"postgresql://{DATABASE_USER}:{PASSWORD}@localhost:5431/{DATABASE}"
engine: Engine = create_engine(conn_str)
metadata = MetaData(engine, schema=PROJECT,)
sessionMaker = sessionmaker(bind=engine)

def _name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    if constraint.name:
        return constraint.name.lower()
    # if this didn't work, revert to the default behavior
    return name_for_collection_relationship(base, local_cls, referred_cls, constraint)

## Map tables to objects
Base = automap_base(metadata=metadata)
Base.prepare(
    reflect=True,
    name_for_collection_relationship=_name_for_collection_relationship
)


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
