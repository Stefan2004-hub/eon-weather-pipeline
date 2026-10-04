from __future__ import annotations

import pytest


@pytest.fixture
def weather_payload() -> dict[str, object]:
    """Small Open-Meteo-shaped payload covering cleaning and aggregation."""

    return {
        "hourly": {
            "time": ["2026-10-04T00:00", "2026-10-04T01:00", "2026-10-05T00:00"],
            "temperature_2m": [10.0, None, 12.0],
            "wind_speed_10m": [3.0, 4.0, 7.0],
            "direct_radiation": [2.0, 3.0, 5.0],
        }
    }
