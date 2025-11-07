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

    # ✅ Fetch Shopify metafields only (Not all fields)
    def fetch_metafields(self, shopify_object: str, shopify_id: str):
        url = f"https://{self.shopify_url}/admin/api/2024-10/{shopify_object}/{shopify_id}/metafields.json"
        headers = {"X-Shopify-Access-Token": self.shopify_token}
        resp = httpx.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("metafields", [])
        return []

    # ✅ Get existing HubSpot properties
    def get_hubspot_properties(self, hubspot_object: str):
        url = f"https://api.hubapi.com/crm/v3/properties/{hubspot_object}s"
        headers = {"Authorization": f"Bearer {self.hubspot_token}"}
        resp = httpx.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return [p["name"] for p in resp.json().get("results", [])]

    # ✅ Create missing HubSpot custom property (only required ones)
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

    # ✅ Sync only mapped fields + metafields
    def send_to_hubspot(self, object_type: str, shopify_obj: dict):
        config = self.config[object_type]
        hubspot_object = config["hubspot_object"]
        unique_field = config["unique_field"]
        property_group = config["property_group"]

        # ✅ Step 1: Map only defined fields (if mapping exists)
        mapping = config.get("field_mapping", {})
        properties = {}
        for shopify_field, hubspot_field in mapping.items():
            properties[hubspot_field] = shopify_obj.get(shopify_field)

        # ✅ Step 2: Add metafields (use exact key name from Shopify)
        metafields = self.fetch_metafields(config["shopify_key"], shopify_obj["id"])
        for meta in metafields:
            meta_key = meta.get("key")   # No renaming (dynamic)
            properties[meta_key] = meta.get("value")

        # ✅ ✅ Step 2.5: Dynamic allowed_values validation (NO HARDCODING)
        allowed_cfg = config.get("allowed_values", {})

        for field, rules in allowed_cfg.items():
            if field in properties:
                value = properties[field]
                allowed = rules.get("allowed", [])
                default = rules.get("default", None)

                if value not in allowed:
                    if default:
                        print(f"⚠ '{value}' not allowed for {field} → using default '{default}'")
                        properties[field] = default
                    else:
                        print(f"⚠ '{value}' not allowed for {field} and no default set → removing field")
                        properties.pop(field, None)

        # ✅ Step 3: Ensure required HubSpot fields exist
        existing_fields = self.get_hubspot_properties(hubspot_object)
        for field in properties.keys():
            if field not in existing_fields:
                self.create_hubspot_property(hubspot_object, field, property_group)
                time.sleep(0.5)

        # ✅ Step 4: Ensure unique field is available
        if not properties.get(unique_field):
            print(f"⚠ Skipping {object_type}: missing {unique_field}")
            return

        # ✅ Step 5: Upsert (create or update) in HubSpot
        search_url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s/search"
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}
        payload = {
            "filterGroups": [{"filters": [{"propertyName": unique_field, "operator": "EQ", "value": properties[unique_field]}]}],
            "limit": 1,
        }
        resp = httpx.post(search_url, headers=headers, json=payload)
        existing = resp.json().get("results", [])

        api_url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s"
        if existing:
            obj_id = existing[0]["id"]
            httpx.patch(f"{api_url}/{obj_id}", headers=headers, json={"properties": properties})
            print(f"🔁 Updated {hubspot_object}: {properties[unique_field]}")
        else:
            resp = httpx.post(api_url, headers=headers, json={"properties": properties})
            if resp.status_code == 201:
                print(f"✅ Created {hubspot_object}: {properties[unique_field]}")
            else:
                print(f"❌ Failed: {resp.text}")

    def sync_all(self):
        for object_type in self.config.keys():
            data = self.fetch_shopify_data(object_type)
            print("data--->", data)
            for item in data:
                self.send_to_hubspot(object_type, item)
