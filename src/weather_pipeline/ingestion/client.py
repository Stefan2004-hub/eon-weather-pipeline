"""Resilient synchronous HTTP client for Open-Meteo."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests


class OpenMeteoClientError(RuntimeError):
    """Raised after a non-retryable or exhausted Open-Meteo request failure."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ApiResponse:
    """Successful HTTP response data needed by the ingestion service."""

    status_code: int
    body: bytes
    payload: dict[str, Any]
    duration_ms: int


class OpenMeteoClient:
    """Request Open-Meteo JSON with bounded retries for transient failures."""

    def __init__(
        self,
        *,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        max_retries: int,
        retry_backoff_seconds: float,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._timeout = (connect_timeout_seconds, read_timeout_seconds)
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._session = session or requests.Session()
        self._sleep = sleep
        self._monotonic = monotonic

    def get_json(self, url: str, parameters: dict[str, str | float | int]) -> ApiResponse:
        """Fetch one JSON object, retrying only transient transport/server errors."""

        for attempt in range(self._max_retries + 1):
            started = self._monotonic()
            try:
                response = self._session.get(url, params=parameters, timeout=self._timeout)
            except requests.RequestException as exc:
                if attempt == self._max_retries:
                    raise OpenMeteoClientError(f"Open-Meteo request failed: {exc}") from exc
                self._sleep(self._backoff(attempt, None))
                continue

            duration_ms = round((self._monotonic() - started) * 1000)
            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_retries:
                    raise OpenMeteoClientError(
                        f"Open-Meteo request failed with HTTP {response.status_code} after retries",
                        status_code=response.status_code,
                    )
                self._sleep(self._backoff(attempt, response.headers.get("Retry-After")))
                continue
            if response.status_code >= 400:
                raise OpenMeteoClientError(
                    f"Open-Meteo request failed with HTTP {response.status_code}",
                    status_code=response.status_code,
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise OpenMeteoClientError("Open-Meteo response was not valid JSON") from exc
            if not isinstance(payload, dict):
                raise OpenMeteoClientError("Open-Meteo response must be a JSON object")
            return ApiResponse(response.status_code, response.content, payload, duration_ms)
        raise AssertionError("unreachable")

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        retry_after_seconds = _parse_retry_after(retry_after)
        if retry_after_seconds is not None:
            return min(retry_after_seconds, 30)
        return min(self._retry_backoff_seconds * (2**attempt), 30)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0, (parsed - datetime.now(UTC)).total_seconds())
