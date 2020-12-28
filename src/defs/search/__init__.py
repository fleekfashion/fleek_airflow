import os
from src.defs.delta.utils import PROJECT

URL = 'http://161.35.113.38/'
PASSWORD = os.environ["SEARCH_PASSWORD"]
PRODUCT_SEARCH_INDEX = f"{PROJECT}_products"
AUTOCOMPLETE_INDEX = f"{PROJECT}_autocomplete"
