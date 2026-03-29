from pathlib import Path

from pydantic import BaseModel

from ingestion.models.enums import DataStageType


class PathConfig(BaseModel):
    base_directory_path: Path
    seeds_directory_name: str
    sources_directory_name: str
    checkpoints_directory_name: str

    def seeds_directory_path_for_stage(self, stage: DataStageType) -> Path:
        return self.base_directory_path / stage / self.seeds_directory_name

    def sources_directory_path_for_stage(self, stage: DataStageType) -> Path:
        return self.base_directory_path / stage / self.sources_directory_name

    @property
    def checkpoints_directory_path(self):
        return self.base_directory_path / Path(self.checkpoints_directory_name)
