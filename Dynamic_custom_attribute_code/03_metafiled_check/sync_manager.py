import os
import json
import re
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

    # ---------------------------
    # Shopify API Call
    # ---------------------------
    def fetch_shopify_data(self, object_type: str):
        endpoint = self.config[object_type]["shopify_endpoint"]
        url = f"https://{self.shopify_url}/admin/api/2025-01/{endpoint}"
        headers = {
            "X-Shopify-Access-Token": self.shopify_token,
            "Content-Type": "application/json",
        }

        print(f"🔄 Fetching Shopify {object_type} data...")
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json().get(f"{object_type}s", [])
        print(f"✅ Found {len(data)} {object_type}(s) in Shopify.")
        print("data---->", data)
        return data

    # ---------------------------
    # Shopify metafields
    # ---------------------------
    def fetch_shopify_metafields(self, object_type: str, object_id: str):
        url = f"https://{self.shopify_url}/admin/api/2025-01/{object_type}s/{object_id}/metafields.json"
        headers = {"X-Shopify-Access-Token": self.shopify_token}
        try:
            response = httpx.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            metafields = response.json().get("metafields", [])
            return {mf["key"]: mf["value"] for mf in metafields}
        except Exception as e:
            print(f"⚠️ Error fetching metafields for {object_type} {object_id}: {e}")
            return {}

    # ---------------------------
    # HubSpot: Fetch existing properties
    # ---------------------------
    def get_hubspot_properties(self, hubspot_object: str):
        url = f"https://api.hubapi.com/crm/v3/properties/{hubspot_object}s"
        headers = {"Authorization": f"Bearer {self.hubspot_token}"}
        resp = httpx.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return [p["name"] for p in resp.json().get("results", [])]

    # ---------------------------
    # HubSpot: Create property if missing
    # ---------------------------
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
            print(f"✅ HubSpot property ready: {name} (group: {group_name})")
        else:
            print(f"❌ Failed to create property {name}: {resp.text}")

    # ---------------------------
    # Ensure HubSpot properties exist
    # ---------------------------
    def ensure_hubspot_properties_exist(self, hubspot_object: str, properties: dict, group_name: str):
        existing_props = self.get_hubspot_properties(hubspot_object)
        for key in properties.keys():
            if key not in existing_props:
                self.create_hubspot_property(hubspot_object, key, group_name)
                time.sleep(1)
    # ---------------------------
    # Check if record exists in HubSpot
    # ---------------------------
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

    # ---------------------------
    # Create or Update HubSpot Record
    # ---------------------------
    def send_to_hubspot(self, object_type: str, shopify_obj: dict):
        config = self.config[object_type]
        mapping = config["field_mapping"]
        hubspot_object = config["hubspot_object"]
        property_group = config.get("property_group", "defaultgroup")

        # Map Shopify fields -> HubSpot properties dynamically
        properties = {}
        for shopify_field, hub_field in mapping.items():
            val = shopify_obj.get(shopify_field)
            if isinstance(val, (dict, list)):
                val = json.dumps(val)  # flatten nested objects
            properties[hub_field] = val

        # Ensure HubSpot properties exist
        self.ensure_hubspot_properties_exist(hubspot_object, properties, property_group)

        # Determine unique field dynamically
        unique_field = config.get("unique_field") or list(mapping.values())[0]
        unique_value = properties.get(unique_field)
        if not unique_value:
            print(f"⚠️ Skipping {object_type}, missing unique value for {unique_field}")
            return

        # Check if record exists
        existing_id = self.get_existing_hubspot_record(hubspot_object, unique_field, unique_value)
        headers = {"Authorization": f"Bearer {self.hubspot_token}", "Content-Type": "application/json"}

        if existing_id:
            url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s/{existing_id}"
            resp = httpx.patch(url, headers=headers, json={"properties": properties}, timeout=30)
            if resp.status_code in (200, 204):
                print(f"🔁 Updated existing {hubspot_object} ({unique_value})")
            else:
                print(f"❌ Update failed for {unique_value}: {resp.text}")
        else:
            url = f"https://api.hubapi.com/crm/v3/objects/{hubspot_object}s"
            resp = httpx.post(url, headers=headers, json={"properties": properties}, timeout=30)
            if resp.status_code == 201:
                print(f"✅ Created new {hubspot_object} ({unique_value})")
            else:
                print(f"❌ Create failed for {unique_value}: {resp.text}")
    # ---------------------------
    # Main sync logic
    # ---------------------------
    def sync_all(self):
        for object_type in self.config.keys():
            try:
                shopify_data = self.fetch_shopify_data(object_type)
                for item in shopify_data:
                    self.send_to_hubspot(object_type, item)
            except Exception as e:
                print(f"⚠️ Error syncing {object_type}: {e}")
