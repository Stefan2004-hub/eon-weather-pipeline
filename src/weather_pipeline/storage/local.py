"""Safe local persistence helpers used by the Phase 1 pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from weather_pipeline.config import Settings


def ensure_output_directories(settings: Settings) -> None:
    """Create the configured data-layer and report parent directories."""

    for directory in (
        settings.bronze_dir,
        settings.silver_dir,
        settings.gold_dir,
        settings.report_path.parent,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Persist an API response as formatted UTF-8 JSON."""

    with path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=4)
