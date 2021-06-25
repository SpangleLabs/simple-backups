import json

from simple_backups.simple_backup import SimpleBackup

with open("config.json", "r") as f:
    config = json.load(f)
simple = SimpleBackup(config)
simple.setup_schedules()
simple.run_scheduler()
