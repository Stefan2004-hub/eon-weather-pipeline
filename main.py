import os
import json
import pandas as pd
from datetime import datetime
from src import (
    BRONZE_DIR,
    GOLD_DIR,
    SILVER_DIR,
    run_bronze_layer,
    run_delivery_report,
    run_gold_layer,
    run_silver_layer,
)


def setup_directories():
    for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        os.makedirs(d, exist_ok=True)


if __name__ == "__main__":
    setup_directories()
    bronze_file = run_bronze_layer()
    silver_file = run_silver_layer(bronze_file)
    gold_file = run_gold_layer(silver_file)
    run_delivery_report(gold_file)
    print("🎉 Pipeline executed end-to-end successfully!")
