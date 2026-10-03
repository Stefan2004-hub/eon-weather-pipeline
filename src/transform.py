import json
import os

import pandas as pd

from .constants import GOLD_DIR, SILVER_DIR


def run_silver_layer(bronze_file_path):
    print("🧹 [Silver] Cleaning and transforming raw data...")
    with open(bronze_file_path, "r") as f:
        raw_data = json.load(f)

    hourly = raw_data.get("hourly", {})
    df = pd.DataFrame(
        {
            "time": pd.to_datetime(hourly.get("time")),
            "temperature_c": hourly.get("temperature_2m"),
            "wind_speed_ms": hourly.get("wind_speed_10m"),
            "direct_radiation": hourly.get("direct_radiation"),
        }
    )

    # Cleaning: Drop rows with missing timestamps or values, normalize units
    df = df.dropna().reset_index(drop=True)

    silver_file_path = os.path.join(SILVER_DIR, "weather_cleaned.csv")
    df.to_csv(silver_file_path, index=False)
    print(f"✅ [Silver] Cleaned dataset saved to {silver_file_path}")
    return silver_file_path


def run_gold_layer(silver_file_path):
    print("🏆 [Gold] Aggregating business-level insights...")
    df = pd.read_csv(silver_file_path)
    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date

    # Business logic: Calculate daily maximum wind speed (relevant for wind turbines)
    # and average temperature/radiation (relevant for solar/heating load)
    gold_df = (
        df.groupby("date")
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            max_wind_speed_ms=("wind_speed_ms", "max"),
            total_direct_radiation=("direct_radiation", "sum"),
        )
        .reset_index()
    )

    gold_file_path = os.path.join(GOLD_DIR, "weather_daily_summary.csv")
    gold_df.to_csv(gold_file_path, index=False)
    print(f"✅ [Gold] Aggregated metrics saved to {gold_file_path}")
    return gold_file_path
