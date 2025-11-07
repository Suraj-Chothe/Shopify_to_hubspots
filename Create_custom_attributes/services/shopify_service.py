import httpx
from core.config import SHOPIFY_URL, SHOPIFY_TOKEN

async def get_shopify_customers():
    """Fetch all Shopify customers."""
    url = f"https://{SHOPIFY_URL}/admin/api/2025-01/customers.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        return data.get("customers", [])


async def get_customer_metafields(customer_id: int):
    """Fetch metafields for a given Shopify customer and return key->value dict."""
    url = f"https://{SHOPIFY_URL}/admin/api/2025-01/customers/{customer_id}/metafields.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        metafields = data.get("metafields", [])
        return {mf.get("key"): mf.get("value") for mf in metafields if mf.get("key") is not None}
