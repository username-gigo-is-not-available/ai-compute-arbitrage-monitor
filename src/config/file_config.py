from pathlib import Path
from pydantic import BaseModel


class FileConfig(BaseModel):
    file_name: str
    subdirectory_name: str | None = None
    input_directory_path: Path | None = None
    output_directory_path: Path | None = None
    checkpoints_directory_path: Path | None = None  # only relevant for streams

    @property
    def input_path(self) -> Path | None:
        if not self.input_directory_path:
            return None
        return self.input_directory_path / self.file_name

    def input_path_with_suffix(self, suffix: str) -> Path | None:
        return self.input_path.with_suffix(suffix)

    @property
    def output_path(self) -> Path | None:
        if not self.output_directory_path:
            return None
        return self.output_directory_path / self.file_name

    def output_path_with_suffix(self, suffix: str) -> Path | None:
        return self.output_path.with_suffix(suffix)

    @property
    def checkpoint_path(self) -> Path | None:
        if not self.checkpoints_directory_path:
            return None
        return self.checkpoints_directory_path / self.subdirectory_name
