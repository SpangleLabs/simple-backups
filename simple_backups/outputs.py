import logging
import time
from abc import ABC, abstractmethod
from typing import Dict

from google.cloud import storage

logger = logging.getLogger(__name__)


class Output(ABC):

    @property
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def send_backup(self, backup_path: str) -> None:
        raise NotImplementedError

    @classmethod
    def from_json(cls, config: Dict) -> 'Output':
        raise NotImplementedError


class GoogleStorage(Output):
    name = "google storage"
    max_attempts = 5
    wait_between_attempts = 20
    upload_timeout = 600

    def __init__(self, bucket_id: str) -> None:
        self.bucket_id = bucket_id
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_id)
        if not self.bucket.versioning_enabled:
            self.bucket.versioning_enabled = True
            self.bucket.patch()

    def send_backup(self, backup_path: str) -> None:
        logger.info(f"Sending backup to google storage bucket {self.bucket_id}")
        blob = self.bucket.blob(backup_path)
        attempts = 0
        last_error = None
        while attempts < self.max_attempts:
            try:
                attempts += 1
                blob.upload_from_filename(filename=backup_path, timeout=self.upload_timeout)
                return
            except ConnectionError as e:
                last_error = e
                time.sleep(self.wait_between_attempts)
                continue
        raise last_error

    @classmethod
    def from_json(cls, config: Dict) -> 'GoogleStorage':
        return cls(config["bucket_id"])


class OutputFactory:
    output_classes = [GoogleStorage]

    def __init__(self) -> None:
        self.names_lookup = {}
        for output in self.output_classes:
            if output.name.casefold() in self.names_lookup:
                raise ValueError(
                    f"Cannot add {output.__name__} output class, as name {output.name} is already "
                    f"used by {self.names_lookup[output.name.casefold()].__name__}"
                )
            self.names_lookup[output.name.casefold()] = output

    def from_json(self, config: Dict) -> Output:
        name = config["type"]
        cls = self.names_lookup.get(name.casefold())
        if cls is None:
            raise ValueError(f"{name} is not a valid output type")
        logger.info(f"Creating output of type: {name}")
        return cls.from_json(config)
