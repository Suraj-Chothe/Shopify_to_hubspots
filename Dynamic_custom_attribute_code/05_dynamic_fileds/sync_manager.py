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
        with open(config_path, "r") as f:
            self.config = json.load(f)

    # ✅ Extract nested values dynamically (e.g., "variants.0.price")
    def get_nested_value(self, data, path):
        keys = str(path).split(".")
        for key in keys:
            if isinstance(data, list):
                try:
                    data = data[int(key)]
                except Exception:
                    return None
            elif isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data

    # ✅ Shopify API call
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

    # ✅ Fetch Shopify metafields (dynamic)
    def fetch_metafields(self, shopify_object: str, shopify_id: str):
        url = f"https://{self.shopify_url}/admin/api/2024-10/{shopify_object}/{shopify_id}/metafields.json"
        headers = {"X-Shopify-Access-Token": self.shopify_token}
        resp = httpx.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("metafields", [])
        return []

    # ✅ Get existing HubSpot properties (to check before creation)
    def get_hubspot_properties(self, hubspot_object: str):
        url = f"https://api.hubapi.com/crm/v3/properties/{hubspot_object}s"
        headers = {"Authorization": f"Bearer {self.hubspot_token}"}
        resp = httpx.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return [p["name"] for p in resp.json().get("results", [])]

    # ✅ Create property only if missing (dynamic)
    def create_hubspot_property(self, hubspot_object: str, name: str, group_name: str):
        url = f"https://api.hubapi.com/crm/v3/properties/{hubspot_object}s"
        payload = {
            "name": name,
            "label": name.capitalize(),
            "type": "string",
            "fieldType": "text",
            "groupName": group_name,
        }
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}
        resp = httpx.post(url, headers=headers, json=payload, timeout=30)

        if resp.status_code in (201, 409):
            print(f"✅ Property ready: {name}")
        else:
            print(f"❌ Failed to create property {name}: {resp.text}")

    # ✅ Map → Validate → Push to HubSpot
    def send_to_hubspot(self, object_type: str, shopify_obj: dict):
        config = self.config[object_type]
        hubspot_object = config["hubspot_object"]
        unique_field = config["unique_field"]
        property_group = config["property_group"]

        # ✅ Step 1: Map fields from JSON (supports nested)
        properties = {}
        mapping = config.get("field_mapping", {})
        for shopify_field, hubspot_field in mapping.items():
            value = self.get_nested_value(shopify_obj, shopify_field)
            properties[hubspot_field] = value

        # ✅ Step 2: Add metafields dynamically
        metafields = self.fetch_metafields(config["shopify_key"], shopify_obj.get("id"))
        for meta in metafields:
            properties[meta["key"]] = meta.get("value")

        # ✅ Step 3: Apply allowed_values dynamically (no hardcoding)
        allowed_cfg = config.get("allowed_values", {})
        for field, rules in allowed_cfg.items():
            if field in properties and properties[field] is not None:
                value = properties[field]
                allowed = rules.get("allowed", [])
                default = rules.get("default", None)
                if allowed and value not in allowed:
                    if default:
                        print(f"⚠ '{value}' not allowed for {field} → using '{default}'")
                        properties[field] = default
                    else:
                        print(f"⚠ '{value}' invalid for {field} → removing field")
                        properties.pop(field, None)

        # ✅ Step 4: Create missing HubSpot fields
        current_properties = self.get_hubspot_properties(hubspot_object)
        for field in list(properties.keys()):
            if field not in current_properties:
                self.create_hubspot_property(hubspot_object, field, property_group)
                time.sleep(0.5)

        # ✅ Step 5: Ensure unique identifier exists
        if not properties.get(unique_field):
            print(f"⚠ Skipping {object_type}: missing unique field '{unique_field}'")
            return

        # ✅ Step 6: Search if record exists → update or create
        search_url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s/search"
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": unique_field,
                    "operator": "EQ",
                    "value": properties[unique_field]
                }]
            }],
            "limit": 1
        }
        resp = httpx.post(search_url, headers=headers, json=payload, timeout=30)
        results = resp.json().get("results", []) if resp.status_code == 200 else []

        api_url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s"
        if results:
            obj_id = results[0]["id"]
            httpx.patch(f"{api_url}/{obj_id}", headers=headers, json={"properties": properties}, timeout=30)
            print(f"🔁 Updated {hubspot_object}: {properties[unique_field]}")
        else:
            r = httpx.post(api_url, headers=headers, json={"properties": properties}, timeout=30)
            if r.status_code == 201:
                print(f"✅ Created {hubspot_object}: {properties[unique_field]}")
            else:
                print(f"❌ Failed: {r.text}")

    # ✅ Main execution (sync all objects from JSON)
    def sync_all(self):
        for object_type in self.config.keys():
            data = self.fetch_shopify_data(object_type)
            # print("data--->", data)
            for item in data:
                self.send_to_hubspot(object_type, item)
