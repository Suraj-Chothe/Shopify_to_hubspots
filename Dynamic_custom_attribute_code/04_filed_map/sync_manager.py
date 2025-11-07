import os
import json
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

class SyncManager:
    def __init__(self, config_path: str):
        self.shopify_url = os.getenv("SHOPIFY_STORE_URL")
        self.shopify_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
        self.hubspot_token = os.getenv("HUBSPOT_ACCESS_TOKEN")

        # Load dynamic config.json
        with open(config_path, "r") as f:
            self.config = json.load(f)

    # ✅ Fetch data from Shopify
    def fetch_shopify_data(self, object_type: str):
        endpoint = self.config[object_type]["shopify_endpoint"]
        url = f"https://{self.shopify_url}/admin/api/2024-10/{endpoint}"
        headers = {"X-Shopify-Access-Token": self.shopify_token}

        print(f"🔄 Fetching Shopify {object_type} data...")
        resp = httpx.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        key = self.config[object_type]["shopify_key"]
        data = resp.json().get(key, [])
        print(f"✅ Found {len(data)} {object_type}(s)")
        return data

    # ✅ Get existing HubSpot properties
    def get_hubspot_properties(self, hubspot_object: str):
        url = f"https://api.hubapi.com/crm/v3/properties/{hubspot_object}s"
        headers = {"Authorization": f"Bearer {self.hubspot_token}"}
        resp = httpx.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return [p["name"] for p in resp.json().get("results", [])]

    # ✅ Create custom HubSpot property dynamically
    def create_hubspot_property(self, hubspot_object: str, name: str, group_name: str):
        url = f"https://api.hubapi.com/crm/v3/properties/{hubspot_object}s"
        payload = {
            "name": name,
            "label": name.capitalize(),
            "type": "string",
            "fieldType": "text",
            "groupName": group_name
        }
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}
        resp = httpx.post(url, headers=headers, json=payload, timeout=30)

        if resp.status_code in (201, 409):
            print(f"✅ Property ready: {name}")
        else:
            print(f"❌ Failed to create property {name}: {resp.text}")

    # ✅ Ensure properties exist
    def ensure_hubspot_properties_exist(self, hubspot_object: str, properties: dict, group_name: str):
        existing = self.get_hubspot_properties(hubspot_object)
        for key in properties.keys():
            if key not in existing:
                self.create_hubspot_property(hubspot_object, key, group_name)
                time.sleep(1)

    # ✅ Check existing record
    def get_existing_hubspot_record(self, hubspot_object: str, unique_field: str, value: str):
        url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s/search"
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}
        payload = {
            "filterGroups": [{"filters": [{"propertyName": unique_field, "operator": "EQ", "value": value}]}],
            "properties": [unique_field],
            "limit": 1
        }
        resp = httpx.post(url, headers=headers, json=payload, timeout=30)

        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0]["id"]
        return None

    # ✅ Create or update HubSpot object
    def send_to_hubspot(self, object_type: str, shopify_obj: dict):
        config = self.config[object_type]
        hubspot_object = config["hubspot_object"]
        property_group = config["property_group"]
        mapping = config["field_mapping"]
        default_values = config.get("default_values", {})
        allowed_cfg = config.get("allowed_values", {})

        # ✅ Map fields Shopify → HubSpot
        properties = {}
        for shopify_field, hubspot_field in mapping.items():
            value = shopify_obj.get(shopify_field)
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            properties[hubspot_field] = value

        # ✅ Add default values from config
        properties.update({k: v for k, v in default_values.items() if properties.get(k) in (None, "")})

        # ✅ Handle invalid currency (dynamic, no hardcoding)
        if "deal_currency_code" in properties:
            allowed = allowed_cfg.get("deal_currency_code", [])
            default_currency = allowed_cfg.get("default_currency", "USD")
            if properties["deal_currency_code"] not in allowed:
                print(f"⚠ Currency {properties['deal_currency_code']} not allowed → using {default_currency}")
                properties["deal_currency_code"] = default_currency

        # ✅ Create missing properties in HubSpot
        self.ensure_hubspot_properties_exist(hubspot_object, properties, property_group)

        # ✅ Get unique field
        unique_field = config["unique_field"]
        unique_value = properties.get(unique_field)
        if not unique_value:
            print(f"⚠ Skipping: No unique field value for {object_type}")
            return

        # ✅ Check existing record
        existing_id = self.get_existing_hubspot_record(hubspot_object, unique_field, unique_value)
        url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s"
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}

        if existing_id:
            httpx.patch(f"{url}/{existing_id}", headers=headers, json={"properties": properties})
            print(f"🔁 Updated {hubspot_object}: {unique_value}")
        else:
            resp = httpx.post(url, headers=headers, json={"properties": properties})
            if resp.status_code == 201:
                print(f"✅ Created new {hubspot_object}: {unique_value}")
            else:
                print(f"❌ Failed: {resp.text}")

    # ✅ Sync all objects (customers, orders, etc.)
    def sync_all(self):
        for object_type in self.config.keys():
            data = self.fetch_shopify_data(object_type)
            for item in data:
                self.send_to_hubspot(object_type, item)

