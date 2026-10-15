"""Orchestrate the established local weather pipeline stages."""

from __future__ import annotations

from uuid import uuid4

from weather_pipeline.config import Settings
from weather_pipeline.ingestion import run_ingestion
from weather_pipeline.ingestion.locations import load_locations
from weather_pipeline.logging import configure_logging, get_logger
from weather_pipeline.models import LocationPipelineArtifacts, PipelineArtifacts
from weather_pipeline.reporting import create_delivery_report
from weather_pipeline.storage import ensure_output_directories
from weather_pipeline.transformations import create_gold_summary, create_silver_dataset


def run_pipeline(settings: Settings | None = None) -> PipelineArtifacts:
    """Ingest and process a forecast independently for every configured location."""

    resolved_settings = settings or Settings()
    configure_logging(resolved_settings.log_level)
    execution_id = uuid4().hex
    logger = get_logger(__name__, execution_id)
    try:
        logger.info("starting pipeline")
        ensure_output_directories(resolved_settings)
        locations = load_locations(resolved_settings.locations_file)
        ingestion = run_ingestion(
            resolved_settings,
            locations,
            dataset="forecast",
            forecast_days=resolved_settings.forecast_days,
            start_date=None,
            end_date=None,
            logger=logger,
        )
        artifacts: list[LocationPipelineArtifacts] = []
        for result in ingestion.results:
            if result.status != "SUCCESS" or result.bronze_path is None:
                continue
            partition = f"source=open_meteo/dataset=forecast/location={result.location_id}"
            silver_file = create_silver_dataset(
                result.bronze_path,
                resolved_settings,
                logger,
                output_path=resolved_settings.silver_dir / partition / "weather_cleaned.csv",
            )
            gold_file = create_gold_summary(
                silver_file,
                resolved_settings,
                logger,
                output_path=resolved_settings.gold_dir / partition / "weather_daily_summary.csv",
            )
            report_file = create_delivery_report(
                gold_file,
                resolved_settings,
                logger,
                output_path=resolved_settings.report_path.parent / partition / "analyst_report.md",
            )
            artifacts.append(
                LocationPipelineArtifacts(result.bronze_path, silver_file, gold_file, report_file)
            )
    except Exception:
        logger.exception("pipeline execution failed", extra={"stage": "orchestration"})
        raise

    logger.info(
        "pipeline completed",
        extra={"successful_locations": len(artifacts), "failed_locations": ingestion.failed_count},
    )
    return PipelineArtifacts(ingestion, tuple(artifacts))
