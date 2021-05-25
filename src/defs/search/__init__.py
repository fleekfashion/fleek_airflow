import os
from src.defs.delta.utils import PROJECT

URL = 'http://159.89.82.234'
PASSWORD = os.environ["SEARCH_PASSWORD"]
PRODUCT_SEARCH_INDEX = f"{PROJECT}_products"
AUTOCOMPLETE_INDEX = f"{PROJECT}_autocomplete"
TRENDING_INDEX = f"{PROJECT}_trending_searches"
LABELS_INDEX = f"{PROJECT}_labels"
