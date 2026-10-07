"""Preserved Pandas transformations for the local weather pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from weather_pipeline.config import Settings
from weather_pipeline.logging import ContextAdapter, with_context


class TransformationError(RuntimeError):
    """Raised when a Bronze or Silver input cannot be transformed safely."""


def create_silver_dataset(bronze_file: Path, settings: Settings, logger: ContextAdapter) -> Path:
    """Clean a Bronze payload into the existing Silver CSV schema."""

    stage_logger = with_context(logger, stage="silver")
    try:
        with bronze_file.open(encoding="utf-8") as file_handle:
            raw_data: dict[str, Any] = json.load(file_handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise TransformationError(f"could not read Bronze file '{bronze_file}': {exc}") from exc

    hourly = raw_data.get("hourly")
    if not isinstance(hourly, dict):
        raise TransformationError("Bronze payload is missing an 'hourly' object")

    required_fields = ("time", "temperature_2m", "wind_speed_10m", "direct_radiation")
    missing_fields = [field for field in required_fields if field not in hourly]
    if missing_fields:
        fields = ", ".join(missing_fields)
        raise TransformationError(f"Bronze payload is missing hourly fields: {fields}")

    try:
        dataframe = (
            pd.DataFrame(
                {
                    "time": pd.to_datetime(hourly["time"]),
                    "temperature_c": hourly["temperature_2m"],
                    "wind_speed_ms": hourly["wind_speed_10m"],
                    "direct_radiation": hourly["direct_radiation"],
                }
            )
            .dropna()
            .reset_index(drop=True)
        )
    except (TypeError, ValueError) as exc:
        raise TransformationError(f"Bronze hourly values cannot form a table: {exc}") from exc

    silver_path = settings.silver_dir / "weather_cleaned.csv"
    dataframe.to_csv(silver_path, index=False)
    stage_logger.info("saved cleaned dataset", extra={"path": silver_path, "rows": len(dataframe)})
    return silver_path


def create_gold_summary(silver_file: Path, settings: Settings, logger: ContextAdapter) -> Path:
    """Aggregate the existing daily energy-relevant metrics into Gold CSV."""

    stage_logger = with_context(logger, stage="gold")
    try:
        dataframe = pd.read_csv(silver_file)
        dataframe["time"] = pd.to_datetime(dataframe["time"])
    except (OSError, KeyError, ValueError, pd.errors.ParserError) as exc:
        raise TransformationError(f"could not read Silver file '{silver_file}': {exc}") from exc

    dataframe["date"] = dataframe["time"].dt.date
    gold_dataframe = (
        dataframe.groupby("date")
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            max_wind_speed_ms=("wind_speed_ms", "max"),
            total_direct_radiation=("direct_radiation", "sum"),
        )
        .reset_index()
    )
    gold_path = settings.gold_dir / "weather_daily_summary.csv"
    gold_dataframe.to_csv(gold_path, index=False)
    stage_logger.info("saved daily summary", extra={"path": gold_path, "rows": len(gold_dataframe)})
    return gold_path
