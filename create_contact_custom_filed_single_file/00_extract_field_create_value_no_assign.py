import os
import httpx
from dotenv import load_dotenv

# -------------------------------------------------------------------
# Load environment variables
# -------------------------------------------------------------------
load_dotenv()

SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")  # e.g. yourstore.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")

# -------------------------------------------------------------------
# Shopify: Fetch all customers
# -------------------------------------------------------------------
def get_shopify_customers():
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
    except Exception as e:
        print(f"⚠️ Error fetching Shopify customers: {e}")
        return []


# -------------------------------------------------------------------
# Shopify: Fetch metafields (custom attributes) for a customer
# -------------------------------------------------------------------
def get_customer_metafields(customer_id):
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2025-01/customers/{customer_id}/metafields.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}

    try:
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        metafields = data.get("metafields", [])
        return {mf["key"]: mf["value"] for mf in metafields}
    except Exception as e:
        print(f"⚠️ Error fetching metafields for customer {customer_id}: {e}")
        return {}


# -------------------------------------------------------------------
# HubSpot: Fetch all contact properties (custom fields)
# -------------------------------------------------------------------
def get_hubspot_properties():
    url = "https://api.hubapi.com/crm/v3/properties/contacts"
    headers = {"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"}

    try:
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        properties = [p["name"] for p in data.get("results", [])]
        return set(properties)
    except Exception as e:
        print(f"⚠️ Error fetching HubSpot properties: {e}")
        return set()


# -------------------------------------------------------------------
# HubSpot: Create a new custom property if it doesn’t exist
# -------------------------------------------------------------------
def create_hubspot_property(name, label):
    url = "https://api.hubapi.com/crm/v3/properties/contacts"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "name": name,
        "label": label.capitalize(),
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation"
    }

    try:
        response = httpx.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 201:
            print(f"🆕 Created new HubSpot property: {name}")
        elif response.status_code == 409:
            print(f"⚠️ Property already exists: {name}")
        else:
            print(f"❌ Failed to create property {name}: {response.text}")
    except Exception as e:
        print(f"⚠️ Error creating HubSpot property {name}: {e}")


# -------------------------------------------------------------------
# HubSpot: Create or update contact with custom properties
# -------------------------------------------------------------------
def create_or_update_hubspot_contact(email, properties):
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {"properties": properties}

    try:
        response = httpx.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 201:
            print(f"✅ Created contact: {email}")
        elif response.status_code == 409:
            print(f"⚠️ Contact already exists: {email}")
        else:
            print(f"❌ Failed to sync contact {email}: {response.text}")
    except Exception as e:
        print(f"⚠️ Error syncing contact {email}: {e}")


# -------------------------------------------------------------------
# Main Sync Logic
# -------------------------------------------------------------------
def main():
    print("🔄 Fetching Shopify customers...")
    customers = get_shopify_customers()
    print("customers---->", customers)
    if not customers:
        print("❌ No customers found.")
        return

    hubspot_props = get_hubspot_properties()
    print("hubspot_props---->", hubspot_props)
    print(f"✅ Loaded {len(hubspot_props)} HubSpot properties.\n")

    for cust in customers:
        firstname = cust.get("first_name")
        lastname = cust.get("last_name")
        email = cust.get("email")
        phone = cust.get("phone")
        customer_id = cust.get("id")

        if not email:
            print("⚠️ Skipping customer without email.")
            continue

        # Fetch custom metafields
        metafields = get_customer_metafields(customer_id)
        print("metafields---->", metafields)
        # Create missing HubSpot properties
        for key in metafields.keys():
            if key not in hubspot_props:
                create_hubspot_property(key, key)
                hubspot_props.add(key)

        # Combine standard + custom fields
        contact_properties = {
            "email": email,
            "firstname": firstname or "",
            "lastname": lastname or "",
        }
        if phone:
            contact_properties["phone"] = phone

        # Add metafields to HubSpot contact properties
        contact_properties.update(metafields)
        print("contact_properties---->", contact_properties)
        # Create or update contact
        create_or_update_hubspot_contact(email, contact_properties)


# -------------------------------------------------------------------
# Run script
# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
