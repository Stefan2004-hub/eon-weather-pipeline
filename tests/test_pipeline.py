from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from weather_pipeline.config import Settings
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
    assert gold["max_wind_speed_ms"].tolist() == [3.0, 7.0]
    assert "Pipeline Status: SUCCESS" in artifacts.report_file.read_text(encoding="utf-8")


def test_pipeline_rejects_missing_hourly_fields(tmp_path: Path) -> None:
    response = Mock()
    response.json.return_value = {"hourly": {"time": []}}

    with patch("weather_pipeline.ingestion.open_meteo.requests.get", return_value=response):
        with pytest.raises(TransformationError, match="missing hourly fields"):
            run_pipeline(Settings(project_root=tmp_path))
