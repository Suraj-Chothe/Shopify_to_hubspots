import os
import httpx
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")  # e.g. simple-test-1.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")


# =========================
# FETCH SHOPIFY ORDERS
# =========================
def get_shopify_orders():
    """Fetch orders from Shopify"""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-01/orders.json?status=any"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("orders", [])


# =========================
# FIND HUBSPOT ORDER BY SHOPIFY ORDER ID
# =========================
def find_hubspot_order(order_id):
    """Check if a HubSpot order already exists for this Shopify order"""
    url = "https://api.hubapi.com/crm/v3/objects/orders/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "filterGroups": [
            {"filters": [{"propertyName": "shopify_order_id", "operator": "EQ", "value": str(order_id)}]}
        ],
        "properties": ["ordername"]
    }

    response = httpx.post(url, headers=headers, json=payload)
    response.raise_for_status()
    results = response.json().get("results", [])
    if results:
        return results[0]["id"]
    return None


# =========================
# CREATE HUBSPOT ORDER
# =========================
def create_hubspot_order(order):
    """Create a HubSpot order for a Shopify order using valid properties"""
    url = "https://api.hubapi.com/crm/v3/objects/orders"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    # Convert Shopify created_at to UTC ISO format for HubSpot
    created_at = order.get("created_at")  # e.g., '2025-10-31T03:07:50-04:00'
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    dt_iso = dt.astimezone(timezone.utc).isoformat()

    shipping_address = order.get("shipping_address") or {}
    city = shipping_address.get("city", "")
    state = shipping_address.get("province", "")
    street = shipping_address.get("address1", "")
    customer_email = order.get("email", "")
    payload = {
        "properties": {
            "hs_order_name": f"Shopify Order #{order['id']}",
            "hs_currency_code": order.get("currency", "USD"),
            "hs_source_store": SHOPIFY_STORE_URL,
            "hs_fulfillment_status": order.get("fulfillment_status", "unknown"),
            "hs_shipping_address_city": city,
            "hs_shipping_address_state": state,
            "hs_shipping_address_street": street
        }
    }

    response = httpx.post(url, headers=headers, json=payload)
    if response.status_code in [200, 201]:
        print(f"✅ Created HubSpot order for Shopify order {order['id']}")
    else:
        print(f"❌ Failed for Shopify order {order['id']}: {response.status_code} - {response.text}")


# =========================
# MAIN FUNCTION
# =========================
def sync_shopify_orders_to_hubspot():
    print("🔄 Fetching Shopify orders...")
    orders = get_shopify_orders()
    print(f"Found {len(orders)} orders.\n")

    for order in orders:
        order_id = order["id"]

        # 1️⃣ Check if order already exists in HubSpot
        existing_order = find_hubspot_order(order_id)
        if existing_order:
            print(f"⚠️ Shopify order {order_id} already exists in HubSpot. Skipping.")
            continue

        # 2️⃣ Create a new order in HubSpot
        create_hubspot_order(order)


if __name__ == "__main__":
    sync_shopify_orders_to_hubspot()
