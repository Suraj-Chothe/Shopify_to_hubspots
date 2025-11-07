import os
import httpx
from dotenv import load_dotenv

load_dotenv()  # Load environment variables

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")

def get_shopify_customers():
    """Fetch customers from Shopify"""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-01/customers.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("customers", [])

def create_hubspot_contact(firstname, lastname, email, phone):
    """Create contact in HubSpot"""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "properties": {
            "firstname": firstname or "",
            "lastname": lastname or "",
            "email": email or "",
            "phone": phone or ""
        }
    }

    response = httpx.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        print(f"✅ Created contact: {email}")
    elif response.status_code == 409:
        print(f"⚠️ Contact already exists: {email}")
    else:
        print(f"❌ Failed for {email}: {response.status_code}, {response.text}")

def main():
    print("🔄 Fetching Shopify customers...")
    customers = get_shopify_customers()
    print("customers", customers)
    print(f"Found {len(customers)} customers.\n")

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