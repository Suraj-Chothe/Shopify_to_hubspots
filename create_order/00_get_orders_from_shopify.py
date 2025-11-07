import os
import httpx
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")  # e.g. simple-test-1.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
# HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")


# =========================
# FETCH SHOPIFY ORDERS
# =========================
def get_shopify_orders():
    """Fetch orders from Shopify"""
    # url = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-01/orders.json"
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-01/orders.json?status=any"
    print("url", url)
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    response = httpx.get(url, headers=headers)
    print("response", response.json())
    response.raise_for_status()
    data = response.json()
    print("data", data)
    return data.get("orders", [])


# =========================
# MAIN FUNCTION
# =========================
def sync_shopify_orders():
    print("🔄 Fetching Shopify orders...")
    orders = get_shopify_orders()
    print("orders", orders)
    print(f"Found {len(orders)} orders.\n")


if __name__ == "__main__":
    sync_shopify_orders()
