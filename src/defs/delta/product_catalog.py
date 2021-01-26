import os
from pyspark.sql.types import *

from src.defs.utils import DeltaTableDef, load_delta_schemas

DATASET = f"product_catalog"

ACTIVE_PRODUCTS_TABLE = DeltaTableDef("active_products", DATASET)
DAILY_PRODUCT_DUMP_TABLE = DeltaTableDef("daily_product_dump", DATASET)
HISTORIC_PRODUCTS_TABLE = DeltaTableDef("historic_products", DATASET)
NEW_PRODUCT_FEATURES_TABLE = DeltaTableDef("daily_product_ml_features", DATASET)
PRODUCT_INFO_TABLE  = DeltaTableDef("product_info", DATASET)
PRODUCT_SIMILARITY_SCORES_TABLE = DeltaTableDef("product_similarity_scores", DATASET)
SIMILAR_PRODUCTS_TABLE = DeltaTableDef("similar_products_v2", DATASET)

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
                            "Labels containing top level about product. "
                            "E.g. pants, top, skirt " 
                            "Never changes"
                        ),
                        "default": []
                    }
                ),
                StructField(name="product_secondary_labels",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Labels containing second level info about product. "
                            "E.g. Denim, Maxi, ruffled "
                            "Never changes"
                        ),
                        "default": []
                    }
                ),
                StructField(name="product_external_labels",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Labels containing external info about product. "
                            "Ussually data from advertiser "
                            "No enforcement, just dump it in, and we will try to use it"
                        ),
                        "default": []
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
                            "Labels containing top level about product. "
                            "E.g. pants, top, skirt " 
                            "Never changes"
                        ),
                        "default": []
                    }
                ),
                StructField(name="product_secondary_labels",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Labels containing second level info about product. "
                            "E.g. Denim, Maxi, ruffled "
                            "Never changes"
                        ),
                        "default": []
                    }
                ),
                StructField(name="product_external_labels",
                    dataType=ArrayType(StringType(), False),
                    nullable=False,
                    metadata={
                        "comment": (
                            "Labels containing external info about product. "
                            "Ussually data from advertiser "
                            "No enforcement, just dump it in, and we will try to use it"
                        ),
                        "default": []
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

load_delta_schemas(TABLES)
