"""Orchestrate the established local weather pipeline stages."""

from __future__ import annotations

from uuid import uuid4

from weather_pipeline.config import Settings
from weather_pipeline.ingestion import extract_weather
from weather_pipeline.logging import configure_logging, get_logger
from weather_pipeline.models import PipelineArtifacts
from weather_pipeline.reporting import create_delivery_report
from weather_pipeline.storage import ensure_output_directories
from weather_pipeline.transformations import create_gold_summary, create_silver_dataset


def run_pipeline(settings: Settings | None = None) -> PipelineArtifacts:
    """Execute Bronze, Silver, Gold, and reporting stages for one forecast request."""

    resolved_settings = settings or Settings()
    configure_logging(resolved_settings.log_level)
    execution_id = uuid4().hex
    logger = get_logger(__name__, execution_id)
    try:
        logger.info("starting pipeline")
        ensure_output_directories(resolved_settings)
        bronze_file = extract_weather(resolved_settings, logger)
        silver_file = create_silver_dataset(bronze_file, resolved_settings, logger)
        gold_file = create_gold_summary(silver_file, resolved_settings, logger)
        report_file = create_delivery_report(gold_file, resolved_settings, logger)
    except Exception:
        logger.exception("pipeline execution failed", extra={"stage": "orchestration"})
        raise

    logger.info("pipeline completed successfully", extra={"report_path": report_file})
    return PipelineArtifacts(bronze_file, silver_file, gold_file, report_file)
