# E.ON Energy Weather Intelligence Pipeline

A local Python weather pipeline for grid-load forecasting and renewable-generation planning. It can extract immutable, multi-location Open-Meteo Bronze data, and its established Phase 1 flow retrieves an Iași forecast, cleans hourly weather data, calculates daily energy-relevant metrics, and produces an analyst-ready Markdown report.

## Pipeline flow

```text
Open-Meteo forecast
  -> Bronze: timestamped raw JSON
  -> Silver: weather_cleaned.csv
  -> Gold: weather_daily_summary.csv
  -> Delivery: analyst_report.md
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

Phase 2 adds raw, independently runnable ingestion. It does not change the existing `run` command or its single-location Silver/Gold/report outputs.

```bash
# Forecasts for every configured location.
uv run weather-pipeline ingest

# A selected forecast location and horizon.
uv run weather-pipeline ingest --location iasi --forecast-days 10

# Historical modeled/reanalysis data for selected locations.
uv run weather-pipeline ingest --dataset historical --location bucharest \
  --start-date 2026-09-01 --end-date 2026-09-30
```

Configured locations live in `config/locations.yaml`: Iași, Bucharest, Cluj-Napoca, Timișoara, and Constanța. Omit `--location` to process all five. Historical requests require inclusive dates, must not be future dates, and are limited to 31 days per command. They are modeled/reanalysis data, not station observations.

Each successful request preserves the raw provider JSON under a unique path such as:

```text
data/bronze/source=open_meteo/dataset=forecast/location=iasi/
  ingestion_date=2026-10-12/<extraction-id>.json
```

`data/bronze/manifest.jsonl` records every successful or failed attempt, including the request, retrieval time, response checksum, HTTP status when available, and Bronze file path. A failure for one location does not stop others; a partial run exits with code 1 so unattended callers do not treat it as fully successful. Forecast payloads are versioned by retrieval time and raw file identity; the provider payload contains weather valid times. The provider does not expose a dependable model issue time here, so the pipeline does not fabricate one.

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
| `WEATHER_PIPELINE_FORECAST_DAYS` | `7` | Default forecast horizon, 1–16 days |
| `WEATHER_PIPELINE_LATITUDE` / `LONGITUDE` | Iași coordinates | Requested location |
| `WEATHER_PIPELINE_TIMEZONE` | `GMT` | Open-Meteo response timezone |
| `WEATHER_PIPELINE_BRONZE_DIR` | `data/bronze` | Raw JSON output directory |
| `WEATHER_PIPELINE_SILVER_DIR` | `data/silver` | Clean CSV output directory |
| `WEATHER_PIPELINE_GOLD_DIR` | `data/gold` | Daily summary output directory |
| `WEATHER_PIPELINE_REPORT_PATH` | `data/analyst_report.md` | Markdown report location |
| `WEATHER_PIPELINE_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |

Relative output paths resolve from the project root, so the CLI can be invoked from another working directory. Bronze files are timestamped; Silver, Gold, and report outputs retain their current overwrite behavior.

## Development

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Tests mock Open-Meteo responses and never require internet access.
