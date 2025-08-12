import argparse
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import click

from simple_backups.simple_backup import SimpleBackup


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(log_level.upper())
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/backups.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)


@click.command()
@click.option("--run-once", type=bool, default=False, help="Run all backups once and exit")
@click.option("--run-sources", type=str, help="Comma separated list of backups to run once and exit", default="")
@click.option("--log-level", type=str, help="Log level for the logger", default="INFO")
def main(
        run_once: bool,
        run_sources: str,
        log_level: str,
):
    setup_logging(log_level)
    with open("config.json", "r") as f:
        config = json.load(f)
    simple = SimpleBackup(config)
    if run_once:
        simple.run_all_backups()
        return
    if run_sources:
        for run_source in run_sources.split(","):
            simple.run_backup_by_name(run_source)
        return
    simple.setup_schedules()
    simple.run_scheduler()


if __name__ == "__main__":
    main()
