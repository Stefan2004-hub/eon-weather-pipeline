from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from weather_pipeline.cli import main
from weather_pipeline.config import Settings
from weather_pipeline.ingestion.open_meteo import IngestionError, extract_weather
from weather_pipeline.logging import get_logger
from weather_pipeline.orchestration import run_pipeline
from weather_pipeline.transformations.weather import TransformationError


def test_pipeline_creates_expected_artifacts_without_network(
    tmp_path: Path, weather_payload: dict[str, object]
) -> None:
    response = Mock()
    response.json.return_value = weather_payload
    settings = Settings(project_root=tmp_path)

    with patch("weather_pipeline.ingestion.open_meteo.requests.get", return_value=response) as get:
        artifacts = run_pipeline(settings)

    assert get.call_args.kwargs["params"]["timezone"] == "GMT"
    assert artifacts.bronze_file.exists()
    assert artifacts.silver_file.name == "weather_cleaned.csv"
    assert artifacts.gold_file.name == "weather_daily_summary.csv"
    assert artifacts.report_file.exists()

    silver = pd.read_csv(artifacts.silver_file)
    assert pd.to_datetime(silver["time"]).tolist() == [
        pd.Timestamp("2026-10-04T00:00:00"),
        pd.Timestamp("2026-10-05T00:00:00"),
    ]
    assert silver["temperature_c"].tolist() == [10.0, 12.0]
    gold = pd.read_csv(artifacts.gold_file)
    assert gold["avg_temperature_c"].tolist() == [10.0, 12.0]
    assert gold["max_wind_speed_ms"].tolist() == [3.0, 7.0]
    assert gold["total_direct_radiation"].tolist() == [2.0, 5.0]
    assert "Pipeline Status: SUCCESS" in artifacts.report_file.read_text(encoding="utf-8")


def test_pipeline_rejects_missing_hourly_fields(tmp_path: Path) -> None:
    response = Mock()
    response.json.return_value = {"hourly": {"time": []}}

    with patch("weather_pipeline.ingestion.open_meteo.requests.get", return_value=response):
        with pytest.raises(TransformationError, match="missing hourly fields"):
            run_pipeline(Settings(project_root=tmp_path))


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (ValueError("invalid JSON"), "not valid JSON"),
        (["not", "an", "object"], "must be a JSON object"),
    ],
)
def test_ingestion_rejects_invalid_api_payloads(
    tmp_path: Path, payload: ValueError | list[str], message: str
) -> None:
    response = Mock()
    if isinstance(payload, Exception):
        response.json.side_effect = payload
    else:
        response.json.return_value = payload

    with patch("weather_pipeline.ingestion.open_meteo.requests.get", return_value=response):
        with pytest.raises(IngestionError, match=message):
            extract_weather(Settings(project_root=tmp_path), get_logger(__name__, "test-run"))


def test_cli_failure_log_retains_execution_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("WEATHER_PIPELINE_PROJECT_ROOT", str(tmp_path))
    caplog.set_level("INFO")

    with patch(
        "weather_pipeline.ingestion.open_meteo.requests.get",
        side_effect=requests.ConnectionError("offline"),
    ):
        assert main(["run"]) == 1

    start_record = next(
        record for record in caplog.records if record.message == "starting pipeline"
    )
    failure_record = next(
        record for record in caplog.records if record.message == "pipeline execution failed"
    )
    assert failure_record.execution_id == start_record.execution_id
    assert failure_record.stage == "orchestration"
    assert not any(record.message == "pipeline completed successfully" for record in caplog.records)


def test_output_directory_failure_is_logged_with_execution_context(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    blocked_bronze_path = tmp_path / "bronze-file"
    blocked_bronze_path.write_text("not a directory", encoding="utf-8")
    caplog.set_level("INFO")

    with pytest.raises(FileExistsError):
        run_pipeline(Settings(project_root=tmp_path, bronze_dir=blocked_bronze_path))

    failure_record = next(
        record for record in caplog.records if record.message == "pipeline execution failed"
    )
    assert failure_record.execution_id
    assert failure_record.stage == "orchestration"
