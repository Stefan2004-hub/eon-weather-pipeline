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

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        duration_ms: int | None = None,
        body: bytes | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.duration_ms = duration_ms
        self.body = body


@dataclass(frozen=True, slots=True)
class ApiResponse:
    """Successful HTTP response data needed by the ingestion service."""

    status_code: int
    body: bytes
    payload: dict[str, Any]
    duration_ms: int
    attempts: int = 1


class OpenMeteoClient:
    """Request Open-Meteo JSON with bounded retries for transient failures."""

    def __init__(
        self,
        *,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        max_retries: int,
        retry_backoff_seconds: float,
        max_retry_after_seconds: float = 300,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._timeout = (connect_timeout_seconds, read_timeout_seconds)
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._owns_session = session is None
        self._session = session or requests.Session()
        self._sleep = sleep
        self._monotonic = monotonic
        self._max_retry_after_seconds = max_retry_after_seconds

    def close(self) -> None:
        """Close the session when this client created it."""
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> OpenMeteoClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def get_json(self, url: str, parameters: dict[str, str | float | int]) -> ApiResponse:
        """Fetch one JSON object, retrying only transient transport/server errors."""

        overall_started = self._monotonic()
        for attempt in range(self._max_retries + 1):
            try:
                response = self._session.get(url, params=parameters, timeout=self._timeout)
            except requests.RequestException as exc:
                if attempt == self._max_retries:
                    raise OpenMeteoClientError(
                        f"Open-Meteo request failed: {exc}",
                        duration_ms=self._duration(overall_started),
                    ) from exc
                self._sleep(self._backoff(attempt, None))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                retry_after = response.headers.get("Retry-After")
                body = response.content
                response.close()
                if attempt == self._max_retries:
                    raise OpenMeteoClientError(
                        f"Open-Meteo request failed with HTTP {response.status_code} after retries",
                        status_code=response.status_code,
                        duration_ms=self._duration(overall_started),
                        body=body,
                    )
                try:
                    delay = self._backoff(attempt, retry_after)
                except OpenMeteoClientError as exc:
                    raise OpenMeteoClientError(
                        str(exc),
                        status_code=response.status_code,
                        duration_ms=self._duration(overall_started),
                        body=body,
                    ) from exc
                self._sleep(delay)
                continue
            if response.status_code >= 400:
                body = response.content
                response.close()
                raise OpenMeteoClientError(
                    f"Open-Meteo request failed with HTTP {response.status_code}",
                    status_code=response.status_code,
                    duration_ms=self._duration(overall_started),
                    body=body,
                )
            try:
                payload = response.json()
            except ValueError as exc:
                body = response.content
                response.close()
                raise OpenMeteoClientError(
                    "Open-Meteo response was not valid JSON",
                    status_code=response.status_code,
                    duration_ms=self._duration(overall_started),
                    body=body,
                ) from exc
            if not isinstance(payload, dict):
                body = response.content
                response.close()
                raise OpenMeteoClientError(
                    "Open-Meteo response must be a JSON object",
                    status_code=response.status_code,
                    duration_ms=self._duration(overall_started),
                    body=body,
                )
            body = response.content
            response.close()
            return ApiResponse(
                response.status_code,
                body,
                payload,
                self._duration(overall_started),
                attempt + 1,
            )
        raise AssertionError("unreachable")

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        retry_after_seconds = _parse_retry_after(retry_after)
        if retry_after_seconds is not None:
            if retry_after_seconds > self._max_retry_after_seconds:
                raise OpenMeteoClientError("Open-Meteo Retry-After exceeds configured maximum wait")
            return retry_after_seconds
        return min(self._retry_backoff_seconds * (2**attempt), 30)

    def _duration(self, started: float) -> int:
        return round((self._monotonic() - started) * 1000)


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
