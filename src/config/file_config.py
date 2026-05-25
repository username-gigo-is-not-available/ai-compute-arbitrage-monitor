from pydantic import BaseModel


class FileConfig(BaseModel):
    file_name: str
    subdirectory_name: str | None = None
    input_directory_path: str | None = None
    output_directory_path: str | None = None

    @property
    def input_path(self) -> str | None:
        if not self.input_directory_path:
            return None
        return f"{self.input_directory_path.rstrip('/')}/{self.file_name}"

    def input_path_with_suffix(self, suffix: str) -> str | None:
        path = self.input_path
        return f"{path}.{suffix.lstrip('.')}" if path else None

    @property
    def output_path(self) -> str | None:
        if not self.output_directory_path:
            return None
        return f"{self.output_directory_path.rstrip('/')}/{self.file_name}"

    def output_path_with_suffix(self, suffix: str) -> str | None:
        path = self.output_path
        return f"{path}.{suffix.lstrip('.')}" if path else None

