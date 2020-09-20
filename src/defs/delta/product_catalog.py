import os
from pyspark.sql.types import *

PROJECT = os.environ.get("PROJECT", "staging")
PROJECT = PROJECT if PROJECT == "prod" else "staging"
DATASET = f"{PROJECT}_product_catalog"

ACTIVE_PRODUCTS_TABLE = "active_products"
DAILY_PRODUCT_DUMP_TABLE = "daily_product_dump"
HISTORIC_PRODUCTS_TABLE = "historic_products"
NEW_PRODUCT_FEATURES_TABLE = "daily_new_product_ml_features"
PRODUCT_INFO_TABLE  = "product_info"
SIMILAR_PRODUCTS_TABLE = "similar_products"

def get_full_name(table_name):
    name = ".".join(
        [
            DATASET,
            table_name
        ]
    )
    return name

def get_columns(table_name):
    return TABLES[table_name]["schema"].fieldNames()


TABLES = {
    ACTIVE_PRODUCTS_TABLE: {
        "schema": StructType([
                StructField(name="product_id",
                    dataType=LongType(),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Unique identifier for each product."
                            "Hash of advertiser_name + image_url"
                        )
                    }
                ),
                StructField(name="advertiser_name",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Name of partner store"
                        )
                    }
                ),
                StructField(name="product_brand",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_name",
                    dataType=StringType(),
                    nullable=False,
                ),
                StructField(name="product_labels",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Tags containing info about product."
                            "Never changes"
                        )
                    }
                ),
                StructField(name="product_tags",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Tags containing metadata for product."
                            "Delete and update daily"
                        )
                    }
                ),
                StructField(name="product_description",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_purchase_url",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_price",
                    dataType=FloatType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_sale_price",
                    dataType=FloatType(),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Default: product_price"
                        )
                    }
                ),
                StructField(name="product_image_url",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_currency",
                    dataType=StringType(),
                    nullable=True,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_additional_image_urls",
                    dataType=ArrayType(StringType(), False),
                    nullable=True,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="advertiser_country",
                    dataType=StringType(),
                    nullable=True,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_image_embedding",
                    dataType=ArrayType(FloatType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Image embedding of product"
                        )
                    }
                ),
                StructField(name="n_views",
                    dataType=IntegerType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        ),
                        "default": 0
                    }
                ),
                StructField(name="n_likes",
                    dataType=IntegerType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        ),
                        "default": 0
                    }
                ),
                StructField(name="n_add_to_cart",
                    dataType=IntegerType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        ),
                        "default": 0
                    }
                ),
                StructField(name="n_conversions",
                    dataType=IntegerType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        ),
                        "default": 0
                    }
                ),
                StructField(name="execution_date",
                    dataType=DateType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="execution_timestamp",
                    dataType=LongType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
        ]),
        "comment": (
            "Current product available in catalog"
        )
    },

    NEW_PRODUCT_FEATURES_TABLE: {
        "schema": StructType([
            StructField(name="product_id",
                dataType=LongType(),
                nullable=False,
                metadata={
                    "comment": (
                        "Unique identifier for each product."
                        "Hash of advertiser_name + image_url"
                    )
                }
            ),
            StructField(name="product_image_embedding",
                dataType=ArrayType(FloatType(), False),
                nullable=False,
                metadata={
                    "comment": (
                        "Image embedding of product"
                    )
                }
            ),
        ]),
        "comment": "Daily ML Features table"
    },

    PRODUCT_INFO_TABLE: {
        "schema": StructType([
                StructField(name="product_id",
                    dataType=LongType(),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Unique identifier for each product."
                            "Hash of advertiser_name + image_url"
                        )
                    }
                ),
                StructField(name="advertiser_name",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Name of partner store"
                        )
                    }
                ),
                StructField(name="product_brand",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_name",
                    dataType=StringType(),
                    nullable=False,
                ),
                StructField(name="product_labels",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Tags containing info about product."
                            "Never changes"
                        )
                    }
                ),
                StructField(name="product_tags",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Tags containing metadata for product."
                            "Delete and update daily"
                        )
                    }
                ),
                StructField(name="product_description",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_purchase_url",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_price",
                    dataType=FloatType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_sale_price",
                    dataType=FloatType(),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Default: product_price"
                        )
                    }
                ),
                StructField(name="product_image_url",
                    dataType=StringType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_currency",
                    dataType=StringType(),
                    nullable=True,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="execution_date",
                    dataType=DateType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="execution_timestamp",
                    dataType=LongType(),
                    nullable=False,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="product_additional_image_urls",
                    dataType=ArrayType(StringType(), False),
                    nullable=True,
                    metadata={
                        "comment": (
                        )
                    }
                ),
                StructField(name="advertiser_country",
                    dataType=StringType(),
                    nullable=True,
                    metadata={
                        "comment": (
                        )
                    }
                ),
        ]),
        "partition": ["execution_date"],
        "comment": (
            "Storing each days post filter product info"
        )
    },

    SIMILAR_PRODUCTS_TABLE: {
        "schema": StructType([
            StructField(name="product_id",
                dataType=LongType(),
                nullable=False,
                metadata={
                    "comment": (
                        "Unique identifier for each product."
                        "Hash of advertiser_name + image_url"
                    )
                }
            ),
            StructField(name="similar_product_ids",
                dataType=ArrayType(LongType(), False),
                nullable=False,
                metadata={
                    "comment": (
                        "Array of product_ids "
                        "Ordered by similarity score"
                    )
                }
            ),
            StructField(name="similarity_scores",
                dataType=ArrayType(FloatType(), False),
                nullable=False,
                metadata={
                    "comment": (
                        "Similarity: range -1 to 1 "
                        "1 is most similar"
                    )
                }
            ),
        ]),
        "comment": (
            "Daily Similar Products table for "
            "all products in our catalog"
        )
    },
}


#######################################
## Similar Tables
#######################################

TABLES[DAILY_PRODUCT_DUMP_TABLE] = {
    "schema": StructType(
        [ StructField(x.name, x.dataType, nullable=True, metadata=x.metadata)
            for x in TABLES[PRODUCT_INFO_TABLE]["schema"].fields
        ]
    ),
    "comment": "Dump products from all sources here"
}

TABLES[HISTORIC_PRODUCTS_TABLE] = {
    "schema": TABLES[ACTIVE_PRODUCTS_TABLE]["schema"],
    "partition": ["execution_date"],
    "comment": "Dump products from all sources here"
}
