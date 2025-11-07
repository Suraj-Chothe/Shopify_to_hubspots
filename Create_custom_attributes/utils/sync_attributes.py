import asyncio
from services.shopify_service import get_shopify_customers, get_customer_metafields
from services.hubspot_service import (
    fetch_hubspot_properties,
    create_hubspot_property,
    create_or_update_hubspot_contact,
)

async def sync_custom_attributes():
    print("🔄 Fetching Shopify customers...")
    customers = await get_shopify_customers()
    if not customers:
        print("❌ No customers found.")
        return

    hubspot_props = await fetch_hubspot_properties("contacts")
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

        # Fetch custom metafields for this customer
        metafields = await get_customer_metafields(customer_id)

        # Ensure all metafield keys exist as HubSpot contact properties
        for key in metafields.keys():
            if key not in hubspot_props:
                status_code, _ = await create_hubspot_property(key, key)
                if status_code == 201:
                    print(f"🆕 Created new HubSpot property: {key}")
                elif status_code == 409:
                    print(f"⚠️ Property already exists: {key}")
                else:
                    print(f"❌ Failed to create property {key}")
                # small delay for propagation
                await asyncio.sleep(1)
                hubspot_props = await fetch_hubspot_properties("contacts")

        # Build contact properties payload
        contact_properties = {
            "email": email,
            "firstname": firstname or "",
            "lastname": lastname or "",
        }
        if phone:
            contact_properties["phone"] = phone

        # include metafields
        contact_properties.update(metafields)

        # Create or update HubSpot contact
        result = await create_or_update_hubspot_contact(email, contact_properties)
        state = result[0] if isinstance(result, tuple) else result
        if state == "created":
            print(f"✅ Created contact: {email}")
        elif state == "updated":
            print(f"🔄 Updated contact: {email}")
        else:
            print(f"❌ Could not sync contact {email}: {result}")
