from __future__ import annotations
from dataclasses import dataclass
import typing as t

from functional import seq

from src.defs.utils import PROJECT, CONN_ID, TableDef, Base

TABLES: t.List[PostgreTable] = list()

@dataclass
class Column:
    name: str 
    type: str
    nullable: bool
    default: t.Optional[t.Any] = None

@dataclass
class PrimaryKey:
    columns: t.List[str]
    name: t.Optional[str] = None
    def get_name(self, table_name: str):
        return self.name or f"pk_{table_name}_{'_'.join(self.columns)}_{PROJECT}"

@dataclass
class Index:
    columns: t.List[str]
    unique: bool = False
    name: t.Optional[str] = None
    def get_name(self, table_name: str):
        prefix = "uq" if self.unique else "ix"
        return self.name or f"{prefix}_{table_name}_{'_'.join(self.columns)}_{PROJECT}"

@dataclass
class ForeignKey:
    columns: t.List[str]
    ref_table: str
    ref_columns: t.List[str]
    name: t.Optional[str] = None
    def get_name(self, table_name: str):
        ref_table_name = self.ref_table.split(".")[-1]
        return self.name or f"fk_{table_name}_{'_'.join(self.columns)}_refs_{ref_table_name}_{PROJECT}"

class PostgreTable(TableDef):
    def __init__(self, 
        name: str, 
        columns: t.List[Column],
        primary_key: t.Optional[PrimaryKey] = None,
        indexes: t.List[Index] = [],
        foreign_keys: t.List[ForeignKey] = []
    ):
        self.name = name
        self.columns = seq(columns)
        self.primary_key = primary_key
        self.indexes = seq(indexes)
        self.foreign_keys = seq(foreign_keys)

    def get_full_name(self, staging=False):
        name = self.name
        if staging:
            name = "staging_" + name
        return f"{self.project}.{name}"

    def get_schema(self) -> seq:
        return self.columns

    def get_columns(self) -> seq:
        return self.columns.map(lambda x: x.name)

    def get_staging_table(self) -> PostgreTable:
        schema = self.get_schema() \
                .map(lambda x: Column(x.name, x.type, nullable=True)) \
                .to_list()
        return PostgreTable(
            "staging_" + self.name,
            schema
        )

    def get_delta_name(self, staging=False) -> str:
        name = "staging_" + self.name if staging else self.name
        return f"{PROJECT}_postgres.{name}"

    def get_orm(self, staging=False):
        name = "staging_" + self.name if staging else self.name
        return Base.classes[name]

