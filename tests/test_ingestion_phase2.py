from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from weather_pipeline.config import Settings
from weather_pipeline.ingestion.client import ApiResponse, OpenMeteoClient, OpenMeteoClientError
from weather_pipeline.ingestion.locations import LocationConfigurationError, load_locations
from weather_pipeline.ingestion.models import Location
from weather_pipeline.ingestion.service import IngestionValidationError, run_ingestion
from weather_pipeline.logging import get_logger


def _payload() -> dict[str, object]:
    return {
        "hourly": {
            "time": ["2026-10-01T00:00"],
            "temperature_2m": [10.0],
            "wind_speed_10m": [3.0],
            "direct_radiation": [2.0],
        }
    }


def _api_response() -> ApiResponse:
    body = json.dumps(_payload(), separators=(",", ":")).encode()
    return ApiResponse(200, body, _payload(), 12)


def test_load_locations_reads_the_committed_catalogue() -> None:
    locations = load_locations(Path("config/locations.yaml"))

    assert [location.id for location in locations] == [
        "iasi",
        "bucharest",
        "cluj-napoca",
        "timisoara",
        "constanta",
    ]
    assert all(location.timezone == "Europe/Bucharest" for location in locations)


def test_load_locations_rejects_duplicate_ids(tmp_path: Path) -> None:
    path = tmp_path / "locations.yaml"
    path.write_text(
        "locations:\n"
        "  - {id: iasi, name: Iasi, latitude: 47, longitude: 27, timezone: GMT}\n"
        "  - {id: iasi, name: Other, latitude: 44, longitude: 26, timezone: GMT}\n",
        encoding="utf-8",
    )

    with pytest.raises(LocationConfigurationError, match="duplicate"):
        load_locations(path)


def test_client_retries_429_and_honors_retry_after() -> None:
    rate_limited = Mock(status_code=429, headers={"Retry-After": "2"})
    success = Mock(status_code=200, content=b'{"hourly": {}}', headers={})
    success.json.return_value = {"hourly": {}}
    session = Mock()
    session.get.side_effect = [rate_limited, success]
    sleeps: list[float] = []
    client = OpenMeteoClient(
        connect_timeout_seconds=1,
        read_timeout_seconds=2,
        max_retries=3,
        retry_backoff_seconds=1,
        session=session,
        sleep=sleeps.append,
    )

    response = client.get_json("https://example.test", {"latitude": 1})

    assert response.status_code == 200
    assert session.get.call_count == 2
    assert sleeps == [2]


def test_client_does_not_retry_permanent_client_errors() -> None:
    response = Mock(status_code=400, headers={})
    session = Mock()
    session.get.return_value = response
    client = OpenMeteoClient(
        connect_timeout_seconds=1,
        read_timeout_seconds=2,
        max_retries=3,
        retry_backoff_seconds=1,
        session=session,
        sleep=Mock(),
    )

    with pytest.raises(OpenMeteoClientError, match="HTTP 400"):
        client.get_json("https://example.test", {})

    assert session.get.call_count == 1


@pytest.mark.parametrize(
    "transient_response",
    [requests.Timeout("slow"), Mock(status_code=503, headers={})],
)
def test_client_retries_timeout_and_server_errors(transient_response: object) -> None:
    success = Mock(status_code=200, content=b'{"hourly": {}}', headers={})
    success.json.return_value = {"hourly": {}}
    session = Mock()
    session.get.side_effect = [transient_response, success]
    sleeps: list[float] = []
    client = OpenMeteoClient(
        connect_timeout_seconds=1,
        read_timeout_seconds=2,
        max_retries=1,
        retry_backoff_seconds=1,
        session=session,
        sleep=sleeps.append,
    )

    client.get_json("https://example.test", {})

    assert session.get.call_count == 2
    assert sleeps == [1]


def test_ingestion_writes_raw_bronze_and_manifest(tmp_path: Path) -> None:
    client = Mock()
    client.get_json.return_value = _api_response()
    location = Location("iasi", "Iași", 47.1585, 27.6014, "Europe/Bucharest")
    settings = Settings(project_root=tmp_path)

    summary = run_ingestion(
        settings,
        (location,),
        dataset="forecast",
        forecast_days=7,
        start_date=None,
        end_date=None,
        logger=get_logger(__name__, "test"),
        client=client,
    )

    result = summary.results[0]
    assert summary.status == "COMPLETED"
    assert result.bronze_path is not None
    assert result.bronze_path.read_bytes() == _api_response().body
    manifest = json.loads(settings.manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "SUCCESS"
    assert manifest["dataset"] == "forecast"
    assert manifest["response_checksum_sha256"]
    assert client.get_json.call_args.args[0] == settings.api_base_url
    assert client.get_json.call_args.args[1]["wind_speed_unit"] == "ms"


def test_ingestion_isolates_location_failures(tmp_path: Path) -> None:
    class SelectiveClient:
        def get_json(self, _url: str, parameters: dict[str, object]) -> ApiResponse:
            if parameters["latitude"] == 44.4268:
                raise OpenMeteoClientError("offline")
            return _api_response()

    locations = (
        Location("iasi", "Iași", 47.1585, 27.6014, "Europe/Bucharest"),
        Location("bucharest", "Bucharest", 44.4268, 26.1025, "Europe/Bucharest"),
    )
    settings = Settings(project_root=tmp_path)
    summary = run_ingestion(
        settings,
        locations,
        dataset="historical",
        forecast_days=None,
        start_date=date(2026, 9, 1),
        end_date=date(2026, 9, 30),
        logger=get_logger(__name__, "test"),
        client=SelectiveClient(),
    )

    assert summary.status == "PARTIAL"
    assert [result.status for result in summary.results] == ["SUCCESS", "FAILED"]
    manifest = [json.loads(line) for line in settings.manifest_path.read_text().splitlines()]
    assert [record["status"] for record in manifest] == ["SUCCESS", "FAILED"]


@pytest.mark.parametrize(
    ("start_date", "end_date", "message"),
    [
        (None, None, "requires start and end"),
        (date(2026, 9, 2), date(2026, 9, 1), "must not precede"),
        (date(2026, 9, 1), date(2026, 10, 2), "must not exceed"),
    ],
)
def test_historical_request_validation(
    tmp_path: Path, start_date: date | None, end_date: date | None, message: str
) -> None:
    with pytest.raises(IngestionValidationError, match=message):
        run_ingestion(
            Settings(project_root=tmp_path),
            (),
            dataset="historical",
            forecast_days=None,
            start_date=start_date,
            end_date=end_date,
            logger=get_logger(__name__, "test"),
            client=Mock(),
        )
