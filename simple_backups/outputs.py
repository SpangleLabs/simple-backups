from abc import ABC, abstractmethod
from typing import Dict

from google.cloud import storage


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

    def __init__(self, bucket_id: str) -> None:
        self.bucket_id = bucket_id
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_id)
        if not self.bucket.versioning_enabled:
            self.bucket.versioning_enabled = True
            self.bucket.patch()

    def send_backup(self, backup_path: str) -> None:
        blob = self.bucket.blob(backup_path)
        blob.upload_from_filename(filename=backup_path)

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
        return cls.from_json(config)
