import argparse
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from simple_backups.simple_backup import SimpleBackup


def read_args():
    parser = argparse.ArgumentParser(description="Simple backup service.")
    parser.add_argument(
        "--run-once",
        help="Run all backups once and exit",
        action="store_true"
    )
    return parser.parse_args()


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/backups.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)


if __name__ == "__main__":
    args = read_args()
    setup_logging()
    with open("config.json", "r") as f:
        config = json.load(f)
    simple = SimpleBackup(config)
    if args.run_once:
        simple.run_all_backups()
    else:
        simple.setup_schedules()
        simple.run_scheduler()
