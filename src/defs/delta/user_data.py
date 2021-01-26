import os
from pyspark.sql.types import *

from src.defs.utils import DeltaTableDef, load_delta_schemas

DATASET = f"user_data"
USER_EVENTS_TABLE = DeltaTableDef("user_events", DATASET)


TABLES = {
    USER_EVENTS_TABLE: {
        "schema": StructType([
            StructField(name="user_id",
                dataType=LongType(),
                nullable=False,
                metadata={
                    "comment": (
                        "Unique identifier for each user."
                    )
                }
            ),
            StructField(name="event",
                dataType=StringType(),
                nullable=False,
                metadata={
                    "comment": (
                        "High level event type."
                    )
                }
            ),
            StructField(name="method",
                dataType=StringType(),
                nullable=True,
                metadata={
                    "comment": (
                        "Method of event."
                    )
                }
            ),
            StructField(name="event_timestamp",
                dataType=LongType(),
                nullable=False,
                metadata={
                }
            ),
            StructField(name="product_id",
                dataType=LongType(),
                nullable=True,
                metadata={
                    "comment": (
                        "Optional identifier for product interaction."
                    )
                }
            ),
            StructField(name="tags",
                dataType=ArrayType(StringType()),
                nullable=True,
                metadata={
                    "comment": (
                        "Optional identifier tags: used for A/B tests"
                    )
                }
            ),
            StructField(name="execution_date",
                dataType=DateType(),
                nullable=False,
                metadata={
                    "comment": (
                        "Optional identifier tags: used for A/B tests"
                    )
                }
            ),
            StructField(name="airflow_execution_timestamp",
                dataType=LongType(),
                nullable=False,
                metadata={
                    "comment": (
                        "used for airflow streaming"
                    )
                }
            ),
            StructField(name="advertiser_names",
                dataType=ArrayType(StringType()),
                nullable=True,
                metadata={
                }
            ),
            StructField(name="product_labels",
                dataType=ArrayType(StringType()),
                nullable=True,
                metadata={
                }
            ),
            StructField(name="searchString",
                dataType=StringType(),
                nullable=True,
                metadata={
                }
            ),
        ]),
        "partition": ["execution_date"],
        "comment": "Table of all user events: event is main event, method is how event occured"
    },
}

load_delta_schemas(TABLES)
