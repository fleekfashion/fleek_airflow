from typing import Dict

import meilisearch
from functional import seq

from src.airflow_tools.databricks.databricks_operators import dbfs_read_json
from src.defs import search

def update_settings(
    synonyms_filepath: str,
    settings_filepath: str,
    index_name: str,
    ) -> Dict[str]:
    index = meilisearch.Client(search.URL, search.PASSWORD) \
            .get_index(index_name)
    settings = dbfs_read_json(settings_filepath)
    settings['synonyms']  = dbfs_read_json(synonyms_filepath) + settings.get('synonyms', [])
    index.update_settings(settings)
    return settings
