from sync_manager import SyncManager

if __name__ == "__main__":
    manager = SyncManager(config_path="config.json")
    manager.sync_all()

