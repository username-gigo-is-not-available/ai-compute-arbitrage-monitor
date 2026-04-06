from datetime import datetime

from pydantic import BaseModel


class BaseRecord(BaseModel):
    ingested_at: datetime
