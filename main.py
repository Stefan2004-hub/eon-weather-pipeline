"""Compatibility entry point for the weather-pipeline CLI."""

from weather_pipeline.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["run"]))
