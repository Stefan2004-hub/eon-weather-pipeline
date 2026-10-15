# E.ON Energy Weather Intelligence Pipeline

A local Python weather pipeline for grid-load forecasting and renewable-generation planning. It retrieves immutable, multi-location Open-Meteo Bronze data, cleans each successful location independently, calculates daily energy-relevant metrics, and produces partitioned analyst-ready reports.

## Pipeline flow

```text
Open-Meteo forecast (all configured locations)
  -> Bronze: immutable raw JSON + per-extraction metadata
  -> Silver: location-partitioned weather_cleaned.csv
  -> Gold: location-partitioned weather_daily_summary.csv
  -> Delivery: location-partitioned analyst_report.md
```

Silver removes rows missing a timestamp, temperature, wind speed, or radiation. Gold retains the established calculations: daily average temperature, maximum wind speed, and summed direct radiation.

## Setup and execution

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-groups
cp .env.example .env
uv run weather-pipeline run
```

Use `uv run weather-pipeline --help` to see available commands. `uv run python main.py` remains available as a compatibility entry point.

## Production-style ingestion

The default `run` command uses the production ingestion engine for every configured location. It continues processing successful locations after isolated ingestion failures and returns a non-zero exit code for a partial or failed run. `ingest` remains available when only raw Bronze extraction is wanted.

```bash
# Forecasts for every configured location.
uv run weather-pipeline ingest

# A selected forecast location and horizon.
uv run weather-pipeline ingest --location iasi --forecast-days 10

# Historical modeled/reanalysis data for selected locations.
uv run weather-pipeline ingest --dataset historical --location bucharest \
  --start-date 2026-09-01 --end-date 2026-09-30
```

Configured locations live in `config/locations.yaml`: Iași, Bucharest, Cluj-Napoca, Timișoara, and Constanța. Omit `--location` to process all five. Historical requests require inclusive dates between 1940-01-01 and today. They are modeled/reanalysis data, not station observations.

Each successful request preserves the raw provider JSON under a unique path such as:

```text
data/bronze/source=open_meteo/dataset=forecast/location=iasi/
  ingestion_date=2026-10-12/<extraction-id>.json
```

`data/bronze/manifest.jsonl` records every successful or failed attempt, including the request, retrieval time, total duration, attempt count, response checksum, HTTP status when available, units, and Bronze file path. Each successful payload has an adjacent atomic metadata record for recovery if the manifest index cannot be written. A manifest-index failure is reported as `FAILED_UNTRACEABLE` and is never silently treated as success.

Weather valid times are requested as Unix seconds in UTC. The configured IANA location timezone is retained as provenance for later local-day aggregation. Forecast issue time is recorded as unavailable because this Open-Meteo integration does not expose a dependable issue timestamp.

## Configuration

Configuration is read from environment variables or an optional `.env` file. Copy `.env.example` and adjust it locally; never commit `.env`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `WEATHER_PIPELINE_API_BASE_URL` | Open-Meteo forecast URL | Forecast endpoint |
| `WEATHER_PIPELINE_ARCHIVE_API_BASE_URL` | Open-Meteo archive URL | Historical endpoint |
| `WEATHER_PIPELINE_LOCATIONS_FILE` | `config/locations.yaml` | Multi-location catalogue |
| `WEATHER_PIPELINE_MANIFEST_PATH` | `data/bronze/manifest.jsonl` | Append-only extraction metadata |
| `WEATHER_PIPELINE_CONNECT_TIMEOUT_SECONDS` / `READ_TIMEOUT_SECONDS` | `5` / `30` | HTTP timeout values |
| `WEATHER_PIPELINE_MAX_RETRIES` | `3` | Retries for transient request failures |
| `WEATHER_PIPELINE_RETRY_BACKOFF_SECONDS` | `1` | Initial exponential-backoff delay |
| `WEATHER_PIPELINE_MAX_RETRY_AFTER_SECONDS` | `300` | Maximum provider-directed wait; longer rate limits fail explicitly rather than being retried early |
| `WEATHER_PIPELINE_FORECAST_DAYS` | `7` | Default forecast horizon, 1–16 days |
| `WEATHER_PIPELINE_LATITUDE` / `LONGITUDE` | Iași coordinates | Requested location |
| `WEATHER_PIPELINE_TIMEZONE` | `GMT` | Open-Meteo response timezone |
| `WEATHER_PIPELINE_BRONZE_DIR` | `data/bronze` | Raw JSON output directory |
| `WEATHER_PIPELINE_SILVER_DIR` | `data/silver` | Clean CSV output directory |
| `WEATHER_PIPELINE_GOLD_DIR` | `data/gold` | Daily summary output directory |
| `WEATHER_PIPELINE_REPORT_PATH` | `data/analyst_report.md` | Markdown report location |
| `WEATHER_PIPELINE_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

Relative output paths resolve from the project root, so the CLI can be invoked from another working directory. Bronze payloads and their sidecar metadata are immutable. Silver, Gold, and reports are partitioned by source, dataset, and location.

## Development

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Tests mock Open-Meteo responses and never require internet access.
