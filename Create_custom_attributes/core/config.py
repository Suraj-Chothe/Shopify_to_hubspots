import os
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_URL = os.getenv("SHOPIFY_STORE_URL")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
