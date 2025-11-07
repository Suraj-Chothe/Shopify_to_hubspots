import httpx
import os
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("HUBSPOT_ACCESS_TOKEN")

response = httpx.get(
    "https://api.hubapi.com/integrations/v1/me",
    headers={"Authorization": f"Bearer {token}"}
)

print(response.status_code, response.text)
