import httpx
from core.config import HUBSPOT_TOKEN

async def fetch_hubspot_properties(object_type: str = "contacts"):
    """Fetch all property names for given object type (e.g., contacts)."""
    url = f"https://api.hubapi.com/crm/v3/properties/{object_type}"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(url, headers=headers)
        res.raise_for_status()
        data = res.json().get("results", [])

    return set(prop.get("name") for prop in data if prop.get("name"))


async def create_hubspot_property(name: str, label: str):
    """Create a custom property on HubSpot contacts object."""
    url = "https://api.hubapi.com/crm/v3/properties/contacts"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "name": name,
        "label": label.capitalize(),
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.post(url, json=payload, headers=headers)
        # Return structured info for caller to decide next steps
        return res.status_code, (res.json() if res.headers.get("content-type", "").startswith("application/json") else res.text)


async def create_or_update_hubspot_contact(email: str, properties: dict):
    """Create HubSpot contact; if exists, update by email."""
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {"properties": properties}

    async with httpx.AsyncClient(timeout=30) as client:
        # Try create
        create_url = "https://api.hubapi.com/crm/v3/objects/contacts"
        create_res = await client.post(create_url, headers=headers, json=payload)

        if create_res.status_code == 201:
            return "created", create_res.json()

        if create_res.status_code != 409:
            return "failed", create_res.status_code, (create_res.text)

        # Exists -> find ID
        search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
        search_payload = {
            "filterGroups": [
                {"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}
            ],
            "properties": ["email"],
        }
        search_res = await client.post(search_url, headers=headers, json=search_payload)
        if search_res.status_code != 200:
            return "failed_search", search_res.status_code, search_res.text

        results = search_res.json().get("results", [])
        if not results:
            return "not_found", None

        contact_id = results[0].get("id")
        if not contact_id:
            return "invalid_id", None

        # Update
        update_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
        update_res = await client.patch(update_url, headers=headers, json=payload)
        if update_res.status_code == 200:
            return "updated", update_res.json()
        return "failed_update", update_res.status_code, update_res.text
