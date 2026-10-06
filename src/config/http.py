import os

from pydantic import BaseModel


class HttpConfig(BaseModel):
    timeout_seconds: int
    retry_count: int
    retry_delay_seconds: int

    @property
    def retry_budget_seconds(self) -> int:
        # The longest all retry waits together can take.
        return self.retry_delay_seconds * self.retry_count
