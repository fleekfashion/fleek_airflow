from typing import Dict, Any, Set, List, Callable

from meilisearch import Client
from meilisearch.index import Index
from functional import seq

from src.airflow_tools.databricks.databricks_operators import dbfs_read_json
from src.defs import search

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

def _process_doc(product_index: Index, doc: dict):
    doc['suggestion'] = f"{doc.get('secondary_attribute', '')} {doc.get('primary_attribute', '')} {doc.get('attribute_descriptor', '')} {doc.get('product_label', '')}".replace("  ", " ").rstrip().lstrip()

    product_label = doc.get('product_label', "")
    search_string = doc['suggestion'].replace(product_label, "")
    opt_params = {
        "offset": 0,
        "limit": 1,
        "attributesToRetrieve": ["product_image_url"]
    }
    if len(product_label) > 0:
        opt_params['facetFilters'] = [f"product_labels:{product_label}"]
    first_hit = product_index.search(search_string, opt_params=opt_params)['hits'][0]
    doc.update(first_hit)


def add_documents(
    def_filepath: str,
    index_name: str,
    ) -> None:
    index = Client(search.URL, search.PASSWORD) \
            .get_index(index_name)
    product_index = Client(search.URL, search.PASSWORD) \
            .get_index(search.PRODUCT_SEARCH_INDEX)
    docs = dbfs_read_json(def_filepath)
    
    for i, doc in enumerate(docs):
        doc["rank"] = i
        _process_doc(product_index, doc)
        doc['primary_key'] = hash(tuple(doc.values()))
    
    print(docs)
    old_keys = _get_keys_to_delete(
            index=index,
            active_keys=seq(docs) \
                    .map(lambda x: x["primary_key"]) \
                    .to_set(),
            primary_key="primary_key"
        )
    index.delete_documents(old_keys)
    index.add_documents(docs)
