import logging
from abc import ABC, abstractmethod
import datetime
from typing import List, Callable, Dict, TYPE_CHECKING

import schedule

if TYPE_CHECKING:
    from simple_backups.sources import Source

logger = logging.getLogger(__name__)


class Schedule(ABC):
    @property
    def names(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def output_subdir(self, backup_timestamp: datetime) -> str:
        raise NotImplementedError

    @abstractmethod
    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        raise NotImplementedError


class Once(Schedule):
    names = ["once", "manual", "run-once"]

    def output_subdir(self, backup_timestamp: datetime) -> str:
        return backup_timestamp.strftime("%Y")

    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        pass


class Monthly(Schedule):
    names = ["monthly", "everymonth"]

    def output_subdir(self, backup_timestamp: datetime) -> str:
        return backup_timestamp.strftime("%Y")

    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        def only_on_first(func: Callable[[], None]):
            if datetime.date.today().day != 1:
                return None
            return func()
        schedule.every().day.at("00:00").do(only_on_first, lambda: job(source))


class Weekly(Schedule):
    names = ["weekly"]

    def output_subdir(self, backup_timestamp: datetime) -> str:
        return backup_timestamp.strftime("%Y")

    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        schedule.every().monday.at("00:00").do(job, source)


class Daily(Schedule):
    names = ["daily", "everyday"]

    def output_subdir(self, backup_timestamp: datetime) -> str:
        return backup_timestamp.strftime("%Y/%m")

    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        schedule.every().day.at("00:00").do(job, source)


class Hourly(Schedule):
    names = ["hourly", "hour"]

    def output_subdir(self, backup_timestamp: datetime) -> str:
        return backup_timestamp.strftime("%Y/%m/%d")

    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        schedule.every().hour.at(":00").do(job, source)


class FiveMinutes(Schedule):
    names = ["5 minutes", "5 mins", "five minutes", "five mins"]

    def output_subdir(self, backup_timestamp: datetime) -> str:
        return backup_timestamp.strftime("%Y/%m/%d")

    def schedule_job(self, job: Callable[['Source'], None], source: 'Source'):
        for m in range(0, 60, 5):
            schedule.every().hour.at(f":{m:02}").do(job, source)


class ScheduleFactory:
    schedule_classes = [Once, Monthly, Weekly, Daily, Hourly, FiveMinutes]

    def __init__(self) -> None:
        self.names_lookup: Dict[str, type] = {}
        for schedule_class in self.schedule_classes:
            for name in schedule_class.names:
                if name.casefold() in self.names_lookup:
                    raise ValueError(
                        f"Cannot add {schedule_class.__name__} schedule class, as name {name} is already "
                        f"used by {self.names_lookup[name.casefold()].__name__}"
                    )
                self.names_lookup[name.casefold()] = schedule_class

    def from_name(self, name: str) -> Schedule:
        cls = self.names_lookup.get(name.casefold())
        if cls is None:
            raise ValueError(f"{name} is not a valid schedule")
        logger.info(f"Creating schedule: {name}")
        return cls()
