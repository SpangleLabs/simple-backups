import json
import logging
import os
import shutil
import sqlite3
import subprocess
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Dict, Optional

import paramiko
import requests

from simple_backups.schedules import Schedule, ScheduleFactory

logger = logging.getLogger(__name__)


class Source(ABC):
    def __init__(self, name: str, schedule: Schedule):
        self.name = name
        self.schedule = schedule

    @property
    def type(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def backup(self, backup_timestamp: datetime) -> str:
        raise NotImplementedError

    def output_path(self, backup_timestamp: datetime, ext: Optional[str]) -> str:
        timestamp = backup_timestamp.strftime("%Y%m%dT%H%M%S")
        filename = timestamp
        if ext:
            filename = f"{timestamp}.{ext}"
        backup_dir = f"backups/{self.name}/{self.schedule.output_subdir(backup_timestamp)}"
        os.makedirs(backup_dir, exist_ok=True)
        return f"{backup_dir}/{filename}"

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'Source':
        raise NotImplementedError


class FileSource(Source):
    type = "file"

    def __init__(self, name: str, schedule: Schedule, file_path: str) -> None:
        super().__init__(name, schedule)
        self.file_path = file_path

    def backup(self, backup_timestamp: datetime) -> str:
        logger.debug(f"Backing up file for source {self.name}")
        file_ext = self.file_path.split(".")[-1]
        output_path = self.output_path(backup_timestamp, file_ext)
        shutil.copy(self.file_path, output_path)
        return output_path

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'FileSource':
        schedule = schedule_factory.from_name(config["schedule"])
        return FileSource(
            config["name"],
            schedule,
            config["path"]
        )


class DirectorySource(Source):
    type = "directory"

    def __init__(self, name: str, schedule: Schedule, dir_path: str):
        super().__init__(name, schedule)
        self.dir_path = dir_path

    def backup(self, backup_timestamp: datetime) -> str:
        logger.info(f"Backing up directory for source {self.name}")
        output_path = self.output_path(backup_timestamp, None)
        shutil.make_archive(output_path, "zip", self.dir_path)
        return f"{output_path}.zip"

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'DirectorySource':
        schedule = schedule_factory.from_name(config["schedule"])
        return cls(
            config["name"],
            schedule,
            config["path"]
        )


class SqliteSource(Source):
    type = "sqlite"

    def __init__(self, name: str, schedule: Schedule, db_path):
        super().__init__(name, schedule)
        self.db_path = db_path

    def backup(self, backup_timestamp: datetime) -> str:
        logger.info(f"Backing up sqlite database for {self.name}")

        def progress(_, remaining, total):
            logger.debug(f"Copied {total - remaining} of {total} pages..")

        output_path = self.output_path(backup_timestamp, "sq3")
        con = sqlite3.connect(self.db_path)
        backup = sqlite3.connect(output_path)
        with backup:
            con.backup(backup, pages=1, progress=progress)
        backup.close()
        con.close()
        return output_path

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'Source':
        schedule = schedule_factory.from_name(config["schedule"])
        return SqliteSource(
            config["name"],
            schedule,
            config["path"]
        )


class SSHRemoteDirectory(Source):
    type = "remote_directory"

    def __init__(self, name: str, schedule: Schedule, host: str, user: str, password: str, path: str):
        super().__init__(name, schedule)
        self.host = host
        self.user = user
        self.password = password
        self.path = path
        self.test_connection()

    def test_connection(self) -> None:
        logger.info(f"Testing ssh connection for: {self.name}")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(self.host, username=self.user, password=self.password)
        finally:
            ssh.close()

    def backup(self, backup_timestamp: datetime) -> str:
        logger.info(f"Backing up remote directory for {self.name}")
        output_path = self.output_path(backup_timestamp, "tar")
        ssh = paramiko.SSHClient()
        try:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host, username=self.user, password=self.password)
            remote_backup_path = f"/tmp/{self.name.replace(' ', '_')}-{backup_timestamp.strftime('%Y%m%dT%H%M%S')}.tar"
            stdin, stdout, stderr = ssh.exec_command(f"tar -cvf {remote_backup_path} {self.path}")
            logger.debug(f"ssh stderr: {stderr.readlines()}")
            logger.debug(f"ssh stdout: {stdout.readlines()}")
            sftp = ssh.open_sftp()
            try:
                sftp.get(remote_backup_path, output_path)
            finally:
                sftp.close()
        finally:
            ssh.close()
        return output_path

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'Source':
        schedule = schedule_factory.from_name(config["schedule"])
        return cls(
            config["name"],
            schedule,
            config["host"],
            config["user"],
            config["pass"],
            config["path"]
        )


class DailysSource(Source):
    type = "dailys"

    def __init__(self, name: str, schedule: Schedule, dailys_url: str, auth_key: str):
        super().__init__(name, schedule)
        self.dailys_url = dailys_url
        self.auth_key = auth_key
        self.test_connection()

    def test_connection(self) -> None:
        logger.info(f"Testing dailys connection for: {self.name}")
        resp = requests.get(
            f"{self.dailys_url}/stats/",
            headers={"Authorization": self.auth_key}
        )
        if resp.status_code != 200:
            raise ValueError(f"Could not authenticate to Dailys system \"{self.name}\". Got response: {resp.content}")

    def backup(self, backup_timestamp: datetime) -> str:
        logger.info(f"Backing up dailys data for {self.name}")
        output_path = self.output_path(backup_timestamp, "json")
        dailys_data = {}
        logger.debug("Getting dailys stat names listing")
        stat_names = requests.get(
            f"{self.dailys_url}/stats/",
            headers={"Authorization": self.auth_key}
        ).json()
        logger.info(f"Getting dailys data for stats: {stat_names}")
        for stat_name in stat_names:
            logger.debug(f"Downloading dailys data for: {stat_name}")
            dailys_data[stat_name] = requests.get(
                f"{self.dailys_url}/stats/{stat_name}/",
                headers={"Authorization": self.auth_key}
            ).json()
        logger.debug("Saving dailys data")
        with open(output_path, "w") as f:
            json.dump(dailys_data, f)
        return output_path

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'Source':
        schedule = schedule_factory.from_name(config["schedule"])
        return cls(
            config["name"],
            schedule,
            config["dailys_url"],
            config["auth_key"]
        )


class MySQLSource(Source):
    type = "mysql"

    def __init__(
            self,
            name: str,
            schedule: Schedule,
            host: Optional[str],
            username: str,
            password: str,
            db_name: str,
            ignore_column_stats: bool = False
    ):
        super().__init__(name, schedule)
        self.host = host
        self.username = username
        self.password = password
        self.db_name = db_name
        self.ignore_column_stats = ignore_column_stats

    def backup(self, backup_timestamp: datetime) -> str:
        logger.info(f"Starting mysql database backup for {self.name}")
        output_file = self.output_path(backup_timestamp, "sql")
        args = [
            f"--user={self.username}",
            f"--password={self.password}",
            f"--result-file={output_file}",
        ]
        if self.ignore_column_stats:
            # Mysqldump 8 will try and dump these by default, and fail if they don't exist
            args.append("--column-statistics=0")
        if self.host:
            args = [f"--host={self.host}"] + args
        args.append(self.db_name)
        process = subprocess.Popen(["mysqldump", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        process.wait()
        if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
            raise Exception(f"mysqldump failed. stderr={err}\nstdout={out}")
        return output_file

    @classmethod
    def from_json(cls, config: Dict, schedule_factory: ScheduleFactory) -> 'Source':
        schedule = schedule_factory.from_name(config["schedule"])
        return cls(
            config["name"],
            schedule,
            config.get("host"),
            config["user"],
            config["pass"],
            config["db_name"],
            config.get("ignore_column_stats", False)
        )


class SourceFactory:
    source_classes = [FileSource, DirectorySource, SqliteSource, SSHRemoteDirectory, DailysSource, MySQLSource]

    def __init__(self) -> None:
        self.names_lookup = {}
        for source in self.source_classes:
            if source.type.casefold() in self.names_lookup:
                raise ValueError(
                    f"Cannot add {source.__name__} source class, as type {source.type} is already "
                    f"used by {self.names_lookup[source.type.casefold()].__name__}"
                )
            self.names_lookup[source.type.casefold()] = source

    def from_json(self, config: Dict, schedule_factory: ScheduleFactory) -> Source:
        name = config["type"]
        cls = self.names_lookup.get(name.casefold())
        if cls is None:
            raise ValueError(f"{name} is not a valid source")
        logger.info(f"Creating source of type: {name}")
        return cls.from_json(config, schedule_factory)
