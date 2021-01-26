from __future__ import annotations
import abc
import os
import typing as t

from functional import seq
from pyspark.sql.types import StructType

PROJECT = os.environ.get("PROJECT", "staging")

class TableDef:
    project = PROJECT

    @abc.abstractmethod
    def get_columns(self) -> seq:
        pass

    @abc.abstractmethod
    def get_full_name(self) -> str:
        pass

class DeltaTableDef(TableDef):

    def __init__(self, schema: StructType, table_name: str, dataset: str):
        self.schema = schema
        self.table_name = table_name
        self.dataset = dataset 

    def get_columns(self) -> seq:
        return seq(self.schema.fieldNames())

    def get_fields(self) -> seq:
        return seq(self.schema.fields)

    def get_full_name(self):
        return f"{self.project}_{self.dataset}.{self.table_name}"

    def get_name(self) -> str:
        return self.table_name

    @classmethod
    def from_tables(cls, name: str, dataset:str, tables: dict) -> DeltaTableDef:
        return cls(
                table_name=name,
                dataset=dataset,
                schema=tables.get(name, dict()).get("schema", StructType())
            )
        
    
class PostgresTableDef(TableDef):
    def __init__(self, schema: t.Dict[str, str], table_name: str):
        self.schema = schema
        self.table_name = table_name

    def get_columns(self) -> seq:
        return seq( [ c['name'] for c in self.schema ] )

    def get_name(self) -> str:
        return self.table_name

    def get_full_name(self):
        return f"{self.project}.{self.table_name}"

