"""Shared typed values passed between pipeline stages."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PipelineArtifacts:
    """Paths created by a successful end-to-end pipeline execution."""

    bronze_file: Path
    silver_file: Path
    gold_file: Path
    report_file: Path
