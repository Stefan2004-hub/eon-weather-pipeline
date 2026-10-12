"""Safe local persistence helpers used by the Phase 1 pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

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


def write_bytes_atomically(path: Path, content: bytes) -> None:
    """Write a raw payload without replacing an existing successful extraction."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"refusing to overwrite Bronze payload {path}")
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary_path.open("xb") as file_handle:
            file_handle.write(content)
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def append_json_line(path: Path, record: dict[str, object]) -> None:
    """Append one machine-readable immutable extraction record."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file_handle:
        file_handle.write(json.dumps(record, sort_keys=True, default=str))
        file_handle.write("\n")
