from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from weather_pipeline.config import Settings


def test_settings_resolve_output_paths_from_project_root(tmp_path: Path) -> None:
    settings = Settings(project_root=tmp_path)

    assert settings.bronze_dir == tmp_path / "data/bronze"
    assert settings.silver_dir == tmp_path / "data/silver"
    assert settings.gold_dir == tmp_path / "data/gold"
    assert settings.report_path == tmp_path / "data/analyst_report.md"
    assert settings.timezone == "GMT"


def test_settings_accept_environment_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("WEATHER_PIPELINE_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("WEATHER_PIPELINE_LATITUDE", "44.4268")
    monkeypatch.setenv("WEATHER_PIPELINE_LOG_LEVEL", "debug")

    settings = Settings()

    assert settings.latitude == 44.4268
    assert settings.log_level == "DEBUG"
    assert settings.bronze_dir == tmp_path / "data/bronze"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 91),
        ("longitude", 181),
        ("timezone", "not/a-timezone"),
        ("log_level", "verbose"),
        ("api_base_url", "not-a-url"),
    ],
)
def test_settings_reject_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})
