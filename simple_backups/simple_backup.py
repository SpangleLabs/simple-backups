import time
from datetime import datetime
from typing import Dict

import schedule

from simple_backups.outputs import OutputFactory
from simple_backups.schedules import ScheduleFactory
from simple_backups.sources import SourceFactory, Source


class SimpleBackup:
    def __init__(self, config: Dict) -> None:
        source_factory = SourceFactory()
        output_factory = OutputFactory()
        schedule_factory = ScheduleFactory()
        self.sources = [source_factory.from_json(source, schedule_factory) for source in config["sources"]]
        self.outputs = [output_factory.from_json(output) for output in config["outputs"]]
        self.heartbeat_url = config["heartbeat_url"]
        self.running = False

    def run_backup(self, source: Source) -> None:
        timestamp = datetime.now()
        backup_path = source.backup(timestamp)
        for output in self.outputs:
            output.send_backup(backup_path)

    def send_heartbeat(self) -> None:
        # TODO
        raise NotImplementedError

    def setup_schedules(self):
        for source in self.sources:
            source.schedule.schedule_job(lambda: self.run_backup(source))
        schedule.every(2).minutes.do(self.send_heartbeat)

    def run_scheduler(self):
        self.running = True
        while self.running:
            schedule.run_pending()
            time.sleep(1)
