"""Markdown report generation for a Gold weather summary."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from weather_pipeline.config import Settings
from weather_pipeline.logging import ContextAdapter, with_context


class ReportingError(RuntimeError):
    """Raised when the analyst report cannot be generated."""


def create_delivery_report(gold_file: Path, settings: Settings, logger: ContextAdapter) -> Path:
    """Create the existing Markdown executive summary from Gold CSV output."""

    stage_logger = with_context(logger, stage="reporting")
    try:
        dataframe = pd.read_csv(gold_file)
    except (OSError, pd.errors.ParserError) as exc:
        raise ReportingError(f"could not read Gold file '{gold_file}': {exc}") from exc

    executive_summary = (
        "This report analyzes recent meteorological parameters to assist grid load forecasting and "
        "renewable generation planning."
    )
    report_content = f"""# E.ON Energy Weather Intelligence Report
Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Executive Summary
{executive_summary}

### Daily Aggregated Metrics:
{dataframe.to_markdown(index=False)}

---
*Pipeline Status: SUCCESS*
"""
    settings.report_path.write_text(report_content, encoding="utf-8")
    stage_logger.info("generated analyst report", extra={"path": settings.report_path})
    return settings.report_path
