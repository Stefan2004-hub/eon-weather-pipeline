# E.ON Energy Weather Intelligence Pipeline

A local Python weather pipeline for grid-load forecasting and renewable-generation planning. It retrieves an Open-Meteo forecast for Iași, cleans hourly weather data, calculates daily energy-relevant metrics, and produces an analyst-ready Markdown report.

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

## Configuration

Configuration is read from environment variables or an optional `.env` file. Copy `.env.example` and adjust it locally; never commit `.env`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `WEATHER_PIPELINE_API_BASE_URL` | Open-Meteo forecast URL | Forecast endpoint |
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
```

Tests mock Open-Meteo responses and never require internet access.
