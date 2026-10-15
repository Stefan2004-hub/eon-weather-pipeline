"""Shared typed values passed between pipeline stages."""

from dataclasses import dataclass
from pathlib import Path

from weather_pipeline.ingestion.models import IngestionRunSummary


@dataclass(frozen=True, slots=True)
class LocationPipelineArtifacts:
    """Downstream files produced for one successful location extraction."""

    bronze_file: Path
    silver_file: Path
    gold_file: Path
    report_file: Path


@dataclass(frozen=True, slots=True)
class PipelineArtifacts:
    """Artifacts and ingestion outcome for a multi-location pipeline run."""

    ingestion: IngestionRunSummary
    locations: tuple[LocationPipelineArtifacts, ...]
