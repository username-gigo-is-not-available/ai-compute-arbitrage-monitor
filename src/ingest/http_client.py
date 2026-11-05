import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, cast

import requests
from tenacity import (
    RetryCallState,
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
)

from config.http import HttpConfig

logger = logging.getLogger(__name__)


# Every ingestor's HTTP GET. A non-200 response or a transport error is retried up to retry_count attempts, waiting
# retry_delay_seconds, or the server's Retry-After when longer. A Retry-After beyond the whole retry budget (Vast.ai's
# daily quota) is not waited out. After the last attempt a non-200 raises requests.HTTPError and a transport error
# re-raises, so the Airflow task fails.
@dataclass
class HttpClient:
    config: HttpConfig
    send: Callable[..., requests.Response] = requests.get
    sleep: Callable[[float], None] = time.sleep

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        response = self.retrying()(self.attempt, url, **kwargs)
        response.raise_for_status()
        return response

    def attempt(self, url: str, **kwargs: Any) -> requests.Response:
        response = self.send(url, timeout=self.config.timeout_seconds, **kwargs)
        if response.status_code != HTTPStatus.OK:
            response.close()
        return response

    def retrying(self) -> Retrying:
        return Retrying(
            stop=stop_after_attempt(self.config.retry_count),
            wait=self.wait_before_retry,
            retry=retry_if_exception_type(requests.RequestException) | retry_if_result(self.should_retry),
            retry_error_callback=self.last_outcome,
            before_sleep=before_sleep_log(cast(Any, logger), logging.WARNING),
            sleep=self.sleep,
        )

    def should_retry(self, response: requests.Response) -> bool:
        return (response.status_code != HTTPStatus.OK
                and not self.config.retry_after_exceeds_budget(response.headers))

    def wait_before_retry(self, retry_state: RetryCallState) -> int:
        if retry_state.outcome.failed:
            return self.config.retry_delay_seconds
        return self.config.retry_wait_seconds(retry_state.outcome.result().headers)

    # After the last attempt: return the last response, so get() raises its HTTPError, or re-raise the last
    # transport error
    @staticmethod
    def last_outcome(retry_state: RetryCallState) -> requests.Response:
        return retry_state.outcome.result()
