from abc import ABC, abstractmethod
from typing import Callable

from pydantic import BaseModel
from common.enums import DataStageType, DatasetType


class BaseStorageConfig(BaseModel, ABC):
    seeds_directory_name: str
    sources_directory_name: str

    @abstractmethod
    def root_uri(self) -> str:
        pass

    def seeds_directory_path_for_stage(self, stage: DataStageType) -> str:
        return f"{self.root_uri()}/{stage.value}/{self.seeds_directory_name}"

    def sources_directory_path_for_stage(self, stage: DataStageType) -> str:
        return f"{self.root_uri()}/{stage.value}/{self.sources_directory_name}"

    def paths_for_dataset_type(self, dataset_type: DatasetType) -> Callable:
        return (
            self.seeds_directory_path_for_stage
            if dataset_type == DatasetType.SEEDS
            else self.sources_directory_path_for_stage
        )

    def paths_for_dataset_stage(self, stage: DataStageType, dataset_type: DatasetType) -> dict:
        path_fn = self.paths_for_dataset_type(dataset_type)
        paths = {"output_directory_path": path_fn(stage)}
        if stage != DataStageType.BRONZE:
            paths["input_directory_path"] = path_fn(DataStageType.BRONZE)
        return paths

class GCPStorageConfig(BaseStorageConfig):
    bucket_name: str

    def root_uri(self) -> str:
        return f"gs://{self.bucket_name.strip('/')}"
