from collections.abc import Mapping

from pydantic import BaseModel


class HttpConfig(BaseModel):
    timeout_seconds: int
    retry_count: int
    retry_delay_seconds: int

    @property
    def retry_budget_seconds(self) -> int:
        return self.retry_delay_seconds * self.retry_count

    @staticmethod
    def retry_after_seconds(headers: Mapping[str, str]) -> int | None:
        retry_after = headers.get("Retry-After", "")
        return int(retry_after) if retry_after.isdigit() else None

    def retry_after_exceeds_budget(self, headers: Mapping[str, str]) -> bool:
        retry_after = self.retry_after_seconds(headers)
        return retry_after is not None and retry_after > self.retry_budget_seconds

    def retry_wait_seconds(self, headers: Mapping[str, str]) -> int:
        return max(self.retry_delay_seconds, self.retry_after_seconds(headers) or 0)
