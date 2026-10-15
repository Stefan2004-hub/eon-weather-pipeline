"""Safe local persistence helpers used by the Phase 1 pipeline."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
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
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary_path.open("xb") as file_handle:
            file_handle.write(content)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            raise FileExistsError(f"refusing to overwrite Bronze payload {path}") from None
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def write_json_atomically(path: Path, record: dict[str, object]) -> None:
    """Write a metadata document atomically without replacing an existing one."""

    write_bytes_atomically(path, json.dumps(record, sort_keys=True, default=str).encode("utf-8"))


@contextmanager
def _exclusive_lock(file_handle):
    """Use an advisory POSIX lock to serialize manifest writers."""

    import fcntl

    fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
    try:
        yield
    finally:
        fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)


def append_json_line(path: Path, record: dict[str, object]) -> None:
    """Append one machine-readable immutable extraction record."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(record, sort_keys=True, default=str) + "\n").encode("utf-8")
    with path.open("ab") as file_handle, _exclusive_lock(file_handle):
        file_handle.write(encoded)
        file_handle.flush()
        os.fsync(file_handle.fileno())
