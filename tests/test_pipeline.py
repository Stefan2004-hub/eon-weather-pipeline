from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weather_pipeline.config import Settings
from weather_pipeline.ingestion.models import ExtractionResult, IngestionRunSummary
from weather_pipeline.orchestration import run_pipeline


def _payload() -> dict[str, object]:
    return {
        "hourly": {
            "time": [1791072000, 1791075600, 1791158400],
            "temperature_2m": [10.0, None, 12.0],
            "wind_speed_10m": [3.0, 4.0, 7.0],
            "direct_radiation": [2.0, 3.0, 5.0],
        },
        "hourly_units": {
            "temperature_2m": "°C",
            "wind_speed_10m": "m/s",
            "direct_radiation": "W/m²",
        },
    }


def test_pipeline_creates_partitioned_artifacts_for_successful_locations(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze.json"
    bronze.write_text(json.dumps(_payload()), encoding="utf-8")
    timestamp = datetime.now(UTC)
    summary = IngestionRunSummary(
        "run",
        timestamp,
        timestamp,
        (
            ExtractionResult("one", "iasi", "forecast", "SUCCESS", bronze),
            ExtractionResult("two", "bucharest", "forecast", "FAILED", None, "offline"),
        ),
    )
    settings = Settings(
        project_root=tmp_path, locations_file=Path("config/locations.yaml").resolve()
    )

    with patch("weather_pipeline.orchestration.pipeline.run_ingestion", return_value=summary):
        artifacts = run_pipeline(settings)

    assert artifacts.ingestion.status == "PARTIAL"
    assert len(artifacts.locations) == 1
    location = artifacts.locations[0]
    assert "location=iasi" in str(location.silver_file)
    assert location.report_file.exists()
    assert pd.read_csv(location.silver_file)["temperature_c"].tolist() == [10.0, 12.0]


def test_pipeline_returns_failed_ingestion_without_downstream_artifacts(tmp_path: Path) -> None:
    timestamp = datetime.now(UTC)
    summary = IngestionRunSummary(
        "run",
        timestamp,
        timestamp,
        (ExtractionResult("one", "iasi", "forecast", "FAILED", None, "offline"),),
    )
    settings = Settings(
        project_root=tmp_path, locations_file=Path("config/locations.yaml").resolve()
    )

    with patch("weather_pipeline.orchestration.pipeline.run_ingestion", return_value=summary):
        artifacts = run_pipeline(settings)

    assert artifacts.ingestion.status == "FAILED"
    assert artifacts.locations == ()
