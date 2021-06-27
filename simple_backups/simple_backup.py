import logging
import time
from datetime import datetime, timedelta
from typing import Dict

import schedule
import heartbeat

from simple_backups.outputs import OutputFactory
from simple_backups.schedules import ScheduleFactory
from simple_backups.sources import SourceFactory, Source

logger = logging.getLogger(__name__)


class SimpleBackup:
    def __init__(self, config: Dict) -> None:
        source_factory = SourceFactory()
        output_factory = OutputFactory()
        schedule_factory = ScheduleFactory()
        self.sources = [source_factory.from_json(source, schedule_factory) for source in config["sources"]]
        self.outputs = [output_factory.from_json(output) for output in config.get("outputs", [])]
        self.running = False
        heartbeat.heartbeat_app_url = config["heartbeat_url"]
        self.heartbeat_id = config["heartbeat_id"]
        heartbeat.initialise_app(self.heartbeat_id, timedelta(minutes=5))
        logger.info("Simple backup instance created")

    def run_backup(self, source: Source) -> None:
        logger.info(f"Creating backup for {source.name}")
        timestamp = datetime.now()
        backup_path = source.backup(timestamp)
        backup_time = (datetime.now()-timestamp).total_seconds()
        logger.info(f"Backup created for {source.name} in {backup_time:.3f} seconds")

        timestamp = datetime.now()
        for output in self.outputs:
            logger.info(f"Sending backup for {source.name} to output: {output.name}")
            output.send_backup(backup_path)
        output_time = (datetime.now()-timestamp).total_seconds()
        total_time = backup_time + output_time
        logger.info(f"Backup output for {source.name} in {output_time:.3f} seconds. Total: {total_time:.3f}s")

    def run_all_backups(self) -> None:
        logger.info("Running all backups")
        for source in self.sources:
            self.run_backup(source)
        logger.info("Backups complete")

    def send_heartbeat(self) -> None:
        logger.info("Sending heartbeat")
        heartbeat.update_heartbeat(self.heartbeat_id)

    def setup_schedules(self) -> None:
        logger.info("Setting up source schedules")
        for source in self.sources:
            source.schedule.schedule_job(self.run_backup, source)
        logger.info("Setting up heartbeat schedule")
        schedule.every(2).minutes.do(self.send_heartbeat)

    def run_scheduler(self):
        self.send_heartbeat()
        self.running = True
        while self.running:
            logger.debug("Checking for schedules")
            schedule.run_pending()
            time.sleep(1)
