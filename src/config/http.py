import os

from pydantic import BaseModel


class HttpConfig(BaseModel):
    timeout_seconds: int
    retry_count: int
    retry_delay_seconds: int
