from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class BaseRecord(BaseModel):
    ingested_at: datetime

    def to_row(self) -> dict:
        return {k: v.value if isinstance(v, Enum) else v for k, v in self.model_dump().items()}
