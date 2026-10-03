import os
import json
import requests
import pandas as pd
from datetime import datetime

# Configuration for a specific location (e.g., Bucharest/Iași coordinates)
LATITUDE = 47.1585
LONGITUDE = 27.6014
API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m,wind_speed_10m,direct_radiation"

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

def setup_directories():
    for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        os.makedirs(d, exist_ok=True)

def run_bronze_layer():
    print("🚀 [Bronze] Extracting raw data from Open-Meteo API...")
    response = requests.get(API_URL)
    response.raise_for_status()
    raw_data = response.json()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(BRONZE_DIR, f"weather_raw_{timestamp}.json")
    
    with open(file_path, "w") as f:
        json.dump(raw_data, f, indent=4)
    
    print(f"✅ [Bronze] Saved raw payload to {file_path}")
    return file_path

def run_silver_layer(bronze_file_path):
    print("🧹 [Silver] Cleaning and transforming raw data...")
    with open(bronze_file_path, "r") as f:
        raw_data = json.load(f)
    
    hourly = raw_data.get("hourly", {})
    df = pd.DataFrame({
        "time": pd.to_datetime(hourly.get("time")),
        "temperature_c": hourly.get("temperature_2m"),
        "wind_speed_ms": hourly.get("wind_speed_10m"),
        "direct_radiation": hourly.get("direct_radiation")
    })
    
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
    gold_df = df.groupby("date").agg(
        avg_temperature_c=("temperature_c", "mean"),
        max_wind_speed_ms=("wind_speed_ms", "max"),
        total_direct_radiation=("direct_radiation", "sum")
    ).reset_index()
    
    gold_file_path = os.path.join(GOLD_DIR, "weather_daily_summary.csv")
    gold_df.to_csv(gold_file_path, index=False)
    print(f"✅ [Gold] Aggregated metrics saved to {gold_file_path}")
    return gold_file_path

def run_delivery_report(gold_file_path):
    print("📤 [Delivery] Generating analyst summary report...")
    df = pd.read_csv(gold_file_path)
    
    report_content = f"""# E.ON Energy Weather Intelligence Report
Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Executive Summary
This report analyzes recent meteorological parameters to assist grid load forecasting and renewable generation planning.

### Daily Aggregated Metrics:
{df.to_markdown(index=False)}

---
*Pipeline Status: SUCCESS*
"""
    
    report_path = "data/analyst_report.md"
    with open(report_path, "w") as f:
        f.write(report_content)
        
    print(f"✅ [Delivery] Report generated successfully at {report_path}")

if __name__ == "__main__":
    setup_directories()
    bronze_file = run_bronze_layer()
    silver_file = run_silver_layer(bronze_file)
    gold_file = run_gold_layer(silver_file)
    run_delivery_report(gold_file)
    print("🎉 Pipeline executed end-to-end successfully!")