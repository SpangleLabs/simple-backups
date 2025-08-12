import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import schedule
import heartbeat
from prometheus_client import start_http_server, Gauge, Histogram

from simple_backups.outputs import OutputFactory
from simple_backups.schedules import ScheduleFactory
from simple_backups.sources import SourceFactory, Source

logger = logging.getLogger(__name__)
source_count = Gauge(
    "simplebackups_source_count",
    "Number of backup sources that are set up and scheduled",
    labelnames=["type", "schedule"]
)
output_count = Gauge(
    "simplebackups_outputs_count",
    "Number of output sources that are set up",
    labelnames=["type"]
)
backup_times = Histogram(
    "simplebackups_backup_time_seconds",
    "Time taken to run each backup and upload the data, in seconds",
    labelnames=["type"],
    buckets=[.5, 1, 2, 5, 10, 20, 60, 300, 600, 30*60]
)


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
        self.prometheus_port = config.get("prometheus_port", 8366)
        heartbeat.initialise_app(self.heartbeat_id, timedelta(minutes=5))
        # Setup metrics
        for source_class in source_factory.source_classes:
            for schedule_class in schedule_factory.schedule_classes:
                source_name = source_class.type
                schedule_name = schedule_class.names[0]
                source_count.labels(type=source_name, schedule=schedule_name).set_function(
                    lambda soc=source_class, scc=schedule_class: len([
                        s for s in self.sources if s.__class__ == soc and s.schedule.__class__ == scc
                    ])
                )
                backup_times.labels(type=source_name)
        for output_class in output_factory.output_classes:
            output_name = output_class.name
            output_count.labels(type=output_name).set_function(
                lambda out=output_class: len([
                    o for o in self.outputs if o.__class__ == out
                ])
            )
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
        backup_times.labels(type=source.type).observe(total_time)
        logger.info(f"Backup output for {source.name} in {output_time:.3f} seconds. Total: {total_time:.3f}s")

    def run_all_backups(self) -> None:
        logger.info("Running all backups")
        for source in self.sources:
            self.run_backup(source)
        logger.info("Backups complete")

    def find_source_by_name(self, source_name: str) -> Optional[Source]:
        for source in self.sources:
            if source.name == source_name:
                return source
        return None

    def run_backup_by_name(self, source_name: str) -> None:
        logger.info(f"Running backup by name {source_name}")
        source = self.find_source_by_name(source_name)
        if source is None:
            raise ValueError(f"Source not found for name: {source_name}")
        self.run_backup(source)

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
        start_http_server(self.prometheus_port)
        self.send_heartbeat()
        self.running = True
        while self.running:
            logger.debug("Checking for schedules")
            schedule.run_pending()
            time.sleep(1)
