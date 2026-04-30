from abc import ABC, abstractmethod
from pydantic import BaseModel
from common.enums import DataStageType


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

class GCPStorageConfig(BaseStorageConfig):
    bucket_name: str

    def root_uri(self) -> str:
        return f"gs://{self.bucket_name.strip('/')}"
