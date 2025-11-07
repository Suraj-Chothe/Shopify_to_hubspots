import os
import httpx
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")  # e.g. test-store-1.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")

# ------------------------- Shopify: Fetch Customers -------------------------
def get_shopify_customers():
    """Fetch all customers from Shopify."""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-01/customers.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }

    try:
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("customers", [])
    except httpx.HTTPStatusError as e:
        print(f"❌ Shopify API error: {e.response.status_code} - {e.response.text}")
        return []
    except Exception as e:
        print(f"⚠️ Unexpected error fetching Shopify customers: {e}")
        return []


# ------------------------- HubSpot: Create Contacts -------------------------
def create_hubspot_contact(firstname, lastname, email, phone=None):
    """Create a contact in HubSpot via Private App token."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "properties": {
            "email": email,
            "firstname": firstname or "",
            "lastname": lastname or "",
        }
    }

    if phone:
        payload["properties"]["phone"] = phone

    try:
        response = httpx.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 201:
            print(f"✅ Created contact: {email}")
        elif response.status_code == 409:
            print(f"⚠️ Contact already exists: {email}")
        elif response.status_code == 401:
            print(f"❌ Unauthorized (check your HUBSPOT_ACCESS_TOKEN): {response.text}")
        else:
            print(f"❌ Failed to create {email}: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"⚠️ Error creating HubSpot contact for {email}: {e}")


# ------------------------- Main Execution -------------------------
def main():
    print("🔄 Fetching Shopify customers...")
    customers = get_shopify_customers()

    if not customers:
        print("❌ No customers found or failed to fetch.")
        return

    print(f"✅ Found {len(customers)} customers.\n")

    for cust in customers:
        firstname = cust.get("first_name")
        lastname = cust.get("last_name")
        email = cust.get("email")
        phone = cust.get("phone")

        if email:
            create_hubspot_contact(firstname, lastname, email, phone)
        else:
            print("⚠️ Skipping customer without email.")


if __name__ == "__main__":
    main()
