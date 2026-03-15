from __future__ import annotations

import threading
import time
from typing import Any

import httpx


class FetchError(RuntimeError):
    def __init__(self, url: str, message: str, *, status_code: int | None = None, error_class: str = "fetch_error") -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.error_class = error_class


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
        payload, _ = self.get_json_with_meta(url, headers=headers)
        return payload

    def get_json_with_meta(self, url: str, headers: dict[str, str] | None = None) -> tuple[Any, int]:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                response = self._client.get(url, headers=headers)
                response.raise_for_status()
                return response.json(), response.status_code
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if attempt >= self.retries:
                    raise FetchError(
                        url,
                        f"HTTP error {status} for {url}",
                        status_code=status,
                        error_class="http_status_error",
                    ) from exc
                last_error = exc
                time.sleep(self.retry_backoff_seconds * (attempt + 1))
            except Exception as exc:  # pragma: no cover - exercised in integration with live sources
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * (attempt + 1))

        raise FetchError(url, f"Failed GET {url}: {last_error}", error_class=type(last_error).__name__) from last_error

    def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> Any:
        result, _ = self.post_json_with_meta(url, payload=payload, headers=headers)
        return result

    def post_json_with_meta(
        self,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> tuple[Any, int]:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                response = self._client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return response.json(), response.status_code
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if attempt >= self.retries:
                    raise FetchError(
                        url,
                        f"HTTP error {status} for {url}",
                        status_code=status,
                        error_class="http_status_error",
                    ) from exc
                last_error = exc
                time.sleep(self.retry_backoff_seconds * (attempt + 1))
            except Exception as exc:  # pragma: no cover - exercised in integration with live sources
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * (attempt + 1))

        raise FetchError(url, f"Failed POST {url}: {last_error}", error_class=type(last_error).__name__) from last_error

    def get_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        text, _ = self.get_text_with_meta(url, headers=headers)
        return text

    def get_text_with_meta(self, url: str, headers: dict[str, str] | None = None) -> tuple[str, int]:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                self._respect_rate_limit()
                response = self._client.get(url, headers=headers)
                response.raise_for_status()
                return response.text, response.status_code
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if attempt >= self.retries:
                    raise FetchError(
                        url,
                        f"HTTP error {status} for {url}",
                        status_code=status,
                        error_class="http_status_error",
                    ) from exc
                last_error = exc
                time.sleep(self.retry_backoff_seconds * (attempt + 1))
            except Exception as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * (attempt + 1))

        raise FetchError(url, f"Failed GET {url}: {last_error}", error_class=type(last_error).__name__) from last_error
