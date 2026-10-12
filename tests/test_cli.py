from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from weather_pipeline.cli import main
from weather_pipeline.ingestion.models import ExtractionResult, IngestionRunSummary


def test_cli_help_exits_successfully(capsys) -> None:
    try:
        main(["--help"])
    except SystemExit as exc:
        assert exc.code == 0

    assert "Run the E.ON weather intelligence pipeline" in capsys.readouterr().out


def test_cli_requires_a_command(capsys) -> None:
    try:
        main([])
    except SystemExit as exc:
        assert exc.code == 2

    assert "the following arguments are required: command" in capsys.readouterr().err


def test_ingest_cli_reports_partial_results(monkeypatch) -> None:
    monkeypatch.setenv("WEATHER_PIPELINE_PROJECT_ROOT", ".")
    timestamp = datetime.now(UTC)
    summary = IngestionRunSummary(
        "run",
        timestamp,
        timestamp,
        (ExtractionResult("extract", "iasi", "forecast", "FAILED", None, "offline"),),
    )
    with patch("weather_pipeline.cli.run_ingestion", return_value=summary):
        assert main(["ingest", "--location", "iasi"]) == 1
