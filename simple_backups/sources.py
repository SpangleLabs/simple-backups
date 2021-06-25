import shutil
import sqlite3
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Dict

from simple_backups.schedules import Schedule, ScheduleFactory


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

    def output_path(self, backup_timestamp: datetime, ext: str) -> str:
        return f"backups/{self.name}/{self.schedule.output_subdir}/{backup_timestamp.isoformat()}.{ext}"


class FileSource(Source):
    type = "file"

    def __init__(self, name: str, schedule: Schedule, file_path: str) -> None:
        super().__init__(name, schedule)
        self.file_path = file_path

    def backup(self, backup_timestamp: datetime) -> str:
        file_ext = self.file_path.split(".")[-1]
        output_path = self.output_path(backup_timestamp, file_ext)
        shutil.copy(self.file_path, output_path)
        return output_path

    @classmethod
    def from_json(cls, config: Dict) -> 'FileSource':
        schedule = ScheduleFactory().from_name(config["schedule"])
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
        output_path = self.output_path(backup_timestamp, "zip")
        shutil.make_archive(output_path, "zip", self.dir_path)
        return output_path


class SqliteSource(Source):
    type = "sqlite"

    def __init__(self, name: str, schedule: Schedule, db_path):
        super().__init__(name, schedule)
        self.db_path = db_path

    def backup(self, backup_timestamp: datetime) -> str:
        def progress(_, remaining, total):
            print(f"Copied {total-remaining} of {total} pages..")

        output_path = self.output_path(backup_timestamp, "sq3")
        con = sqlite3.connect(self.db_path)
        backup = sqlite3.connect(output_path)
        with backup:
            con.backup(backup, pages=1, progress=progress)
        backup.close()
        con.close()
        return output_path


class SourceFactory:
    source_classes = [FileSource, DirectorySource, SqliteSource]

    def __init__(self) -> None:
        self.names_lookup = {}
        for source in self.source_classes:
            if source.type.casefold() in self.names_lookup:
                raise ValueError(
                    f"Cannot add {source.__name__} source class, as type {source.type} is already "
                    f"used by {self.names_lookup[source.type.casefold()].__name__}"
                )
            self.names_lookup[source.type.casefold()] = source

    def from_json(self, config: Dict) -> Source:
        name = config["type"]
        cls = self.names_lookup.get(name.casefold())
        if cls is None:
            raise ValueError(f"{name} is not a valid source")
        return cls.from_json(config)
