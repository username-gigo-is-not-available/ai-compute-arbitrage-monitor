from pathlib import Path


from config.file_config import FileConfig


class OpenDBConfig(FileConfig):
    enabled: bool

    @property
    def open_db_path(self):
        return self.input_directory_path / Path(self.subdirectory_name)
