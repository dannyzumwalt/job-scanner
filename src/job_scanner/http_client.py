from __future__ import annotations

import threading
import time
from typing import Any

import httpx


class HttpFetcher:
    def __init__(
        self,
        timeout_seconds: float = 25.0,
        retries: int = 2,
        retry_backoff_seconds: float = 0.5,
        min_request_interval_seconds: float = 0.25,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.min_request_interval_seconds = min_request_interval_seconds
        self._lock = threading.Lock()
        self._last_request_time = 0.0
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def _respect_rate_limit(self) -> None:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self.min_request_interval_seconds:
                time.sleep(self.min_request_interval_seconds - elapsed)
            self._last_request_time = time.time()

    def get_json(self, url: str, headers: dict[str, str] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                response = self._client.get(url, headers=headers)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # pragma: no cover - exercised in integration with live sources
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * (attempt + 1))

        raise RuntimeError(f"Failed GET {url}: {last_error}") from last_error

    def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                response = self._client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # pragma: no cover - exercised in integration with live sources
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * (attempt + 1))

        raise RuntimeError(f"Failed POST {url}: {last_error}") from last_error
