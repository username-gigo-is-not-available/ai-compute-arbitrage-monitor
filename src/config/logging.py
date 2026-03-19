import logging

from pydantic import BaseModel


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def setup(self):
        return logging.basicConfig(
            level=self.level,
            format=self.format,
            force=True
        )
