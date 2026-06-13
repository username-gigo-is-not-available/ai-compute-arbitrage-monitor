from pydantic import BaseModel


class DbtConfig(BaseModel):
    project_directory_path: str
    profiles_directory_path: str
    target_directory_path: str