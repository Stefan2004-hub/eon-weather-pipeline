"""Application settings loaded from environment variables and ``.env``."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    """Validated runtime configuration for one local pipeline execution."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_prefix="WEATHER_PIPELINE_",
        extra="ignore",
        validate_default=True,
    )

    project_root: Path = PROJECT_ROOT
    api_base_url: str = "https://api.open-meteo.com/v1/forecast"
    latitude: float = Field(default=47.1585, ge=-90, le=90)
    longitude: float = Field(default=27.6014, ge=-180, le=180)
    timezone: str = "GMT"
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    gold_dir: Path = Path("data/gold")
    report_path: Path = Path("data/analyst_report.md")
    log_level: str = "INFO"

    @field_validator("project_root", mode="before")
    @classmethod
    def resolve_project_root(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    @field_validator("bronze_dir", "silver_dir", "gold_dir", "report_path", mode="before")
    @classmethod
    def resolve_project_paths(cls, value: str | Path, info) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path
        project_root = Path(info.data.get("project_root", PROJECT_ROOT))
        return project_root / path

    @field_validator("api_base_url")
    @classmethod
    def validate_api_base_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("must be an absolute HTTP(S) URL")
        return value

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            message = "must be an IANA timezone, such as 'GMT' or 'Europe/Bucharest'"
            raise ValueError(message) from exc
        return value

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise ValueError("must be DEBUG, INFO, WARNING, or ERROR")
        return normalized
