from typing import Dict, Any, Set, List, Callable
import random

from meilisearch import Client
from meilisearch.index import Index
from functional import seq
import sqlalchemy as s

from src.airflow_tools.databricks.databricks_operators import dbfs_read_json
from src.defs import search
from src.defs.postgre.utils import PostgreTable 
from src.defs.postgre import query_utils as qutils 

def _get_keys_to_delete(index: Index, active_keys: Set[int], primary_key: str) -> List[int]:
    hits = index.search("", opt_params={
        "offset": 0,
        "limit": 10**10,
        "attributesToRetrieve": [primary_key]
    })['hits']
    old_keys = seq(hits) \
            .map(lambda x: x[primary_key]) \
            .filter(lambda x: x not in active_keys) \
            .to_list()
    return old_keys

def _add_url(product_index: Index, doc: dict, pg_table):
    doc['suggestion'] = f"{doc.get('secondary_attribute', '')} {doc.get('primary_attribute', '')} {doc.get('attribute_descriptor', '')} {doc.get('product_label', '')}".replace("  ", " ").rstrip().lstrip()

    search_string = doc['suggestion']
    opt_params = {
        "offset": 0,
        "limit": 1,
        "attributesToRetrieve": ["product_image_url"]
    }

    product_label = doc.get('product_label', "")
    if len(product_label) > 0:
        opt_params['facetFilters'] = [f"product_labels:{product_label}"]
    url = product_index.search(search_string, opt_params=opt_params)['hits'][0]
    return {**doc, 'product_image_url': url}


def add_documents(
    def_filepath: str,
    pg_table: PostgreTable,
    processor: Callable = lambda x: x,
    random_order: bool = False
    ) -> None:

    t = pg_table.get_orm()
    product_index = Client(search.URL, search.PASSWORD) \
            .get_index(search.PRODUCT_SEARCH_INDEX)
    docs: list = dbfs_read_json(def_filepath) # type: ignore
    docs = processor(docs)
    for i, doc in enumerate(docs):
        doc["rank"] = random.random() if random_order else i

    def _proc_doc(doc):
        return { key:value for key, value in doc.items()
            if key in pg_table.get_columns() 
        }
    res_docs = seq(docs) \
        .map(lambda doc: _add_url(product_index, doc, pg_table)) \
        .map(_proc_doc) \
        .map(lambda x: t(**x)) \
        .to_list()

    delete_docs = s.delete(t).where(True == True)
    with qutils.session_scope() as session:
        session.execute(delete_docs)
        session.add_all(res_docs)
        session.commit()
