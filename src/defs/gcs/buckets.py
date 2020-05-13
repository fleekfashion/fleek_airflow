import os

PREFIX = "gs://"
MAIN = os.environ.get("GOOGLE_CLOUD_PROJECT", "fleek-staging")
EXPORTS = f"{MAIN}-exports"
IMPORTS = f"{MAIN}-imports"
