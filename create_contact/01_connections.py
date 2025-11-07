import os
import requests
from hubspot import HubSpot
from dotenv import load_dotenv
import json
load_dotenv()

# --------------------------
# ✅ Environment Variables
# --------------------------
SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")   # example: yourstore.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
print(SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN, HUBSPOT_API_KEY)
# --------------------------
# ✅ Initialize HubSpot Client
# --------------------------
hubspot = HubSpot(access_token=HUBSPOT_API_KEY)
print(hubspot)
# --------------------------
# ✅ Fetch Shopify Customers
# --------------------------
def get_shopify_customers():
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2023-10/customers.json"

    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    return response.json()["customers"]

# --------------------------
# ✅ Sync Customer → HubSpot
# --------------------------
def sync_customer_to_hubspot(customer):
    print("Customer keys:", customer.keys())
    print("Customer JSON:", json.dumps(customer, indent=2))
    email = customer.get("email")

    # Skip if no email (HubSpot needs email)
    if not email:
        print("⚠️ Skipping customer without email")
        return

    contact_data = {
        "properties": {
            "email": email,
            "firstname": customer.get("first_name"),
            "lastname": customer.get("last_name"),
            "phone": customer.get("phone"),
            "shopify_customer_id": str(customer.get("id")),
        }
    }

    try:
        # Try creating new HubSpot contact
        hubspot.crm.contacts.basic_api.create(data=contact_data)
        print(f"✅ Created in HubSpot: {email}")

    except Exception as e:
        # If already exists, update contact
        if "CONTACT_ALREADY_EXISTS" in str(e):
            search = hubspot.crm.contacts.search_api.do_search(
                body={
                    "filterGroups": [{
                        "filters": [{
                            "value": email,
                            "propertyName": "email",
                            "operator": "EQ"
                        }]
                    }]
                }
            )

            contact_id = search.results[0].id

            hubspot.crm.contacts.basic_api.update(
                contact_id, body=contact_data
            )
            print(f"♻️ Updated in HubSpot: {email}")
        else:
            print(f"❌ Error for {email}: {e}")

# --------------------------
# ✅ Run Full Sync
# --------------------------
def run_sync():
    print("📦 Fetching customers from Shopify...")
    customers = get_shopify_customers()
    print("customers", customers)
    print(f"✅ Found {len(customers)} customers. Syncing now...\n")
    
    for customer in customers:
        sync_customer_to_hubspot(customer)

    print("\n🎉 Sync completed!")

# --------------------------
# ▶️ Start
# --------------------------
if __name__ == "__main__":
    run_sync()