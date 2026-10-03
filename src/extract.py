from datetime import datetime
import json
import os

import requests

from .constants import BRONZE_DIR

# Configuration for a specific location (e.g., Bucharest/Iași coordinates)
LATITUDE = 47.1585
LONGITUDE = 27.6014
API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m,wind_speed_10m,direct_radiation"


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
