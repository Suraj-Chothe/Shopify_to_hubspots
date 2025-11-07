import asyncio
from utils.sync_attributes import sync_custom_attributes

async def main():
    print("🔄 Syncing custom attributes between Shopify and HubSpot...")
    await sync_custom_attributes()
    print("✅ Sync complete!")

if __name__ == "__main__":
    asyncio.run(main())
