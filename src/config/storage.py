from abc import ABC, abstractmethod

from pydantic import BaseModel

from common.classes import Dataset
from common.enums import DataStageType


class BaseStorageConfig(BaseModel, ABC):
    @abstractmethod
    def root_uri(self) -> str:
        pass

    def path_interpolation(self, stage: DataStageType, dataset: Dataset) -> str:
        return f"{self.root_uri()}/{stage.value.lower()}/{dataset.dataset_type.lower()}/{dataset.dataset_name.lower()}"

    def directory_path(self, stage: DataStageType, dataset: Dataset) -> str:
        return self.path_interpolation(stage=stage, dataset=dataset)

    def directory_path_with_extension(self, stage: DataStageType, dataset: Dataset, file_extension: str = "parquet") -> str:
        return f"{self.directory_path(stage=stage, dataset=dataset).lstrip('.')}.{file_extension}"

class GCPStorageConfig(BaseStorageConfig):
    bucket_name: str

    def root_uri(self) -> str:
        return f"gs://{self.bucket_name.strip('/')}"
