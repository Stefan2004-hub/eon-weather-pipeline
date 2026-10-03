# E.ON Energy Weather Intelligence Pipeline

A modular Python data pipeline built using the **Medallion Architecture** (Bronze, Silver, Gold layers) designed to ingest real-time meteorological data, clean and transform it, aggregate business-level insights, and generate an executive report for grid load and renewable energy forecasting.

## 🏗️ Architecture Overview

```text
[ Open-Meteo API ] 
        │
        ▼ (Raw JSON payload)
   📁 Bronze Layer (Immutable historical raw storage)
        │
        ▼ (Parsing, cleaning, and schema enforcement)
   📁 Silver Layer (Cleaned tabular data)
        │
        ▼ (Aggregation & Business metrics: Wind max, Avg Temp)
   📁 Gold Layer (Analyst-ready daily summaries)
        │
        ▼
   📄 Delivery Report (Markdown Analyst Summary)
Bronze (data/bronze/): Stores raw, unmodified JSON responses directly fetched from the weather API along with a UTC execution timestamp.

Silver (data/silver/): Parses the hourly forecast streams, normalizes columns, drops nulls, and persists a clean CSV dataset.

Gold (data/gold/): Aggregates data by date—calculating peak wind speeds (crucial for wind turbine monitoring) and average temperatures/radiation (relevant for solar and grid heating load).

Delivery (data/analyst_report.md): Generates an executive summary markdown report featuring formatted tables ready for distribution to energy analysts.

🚀 Quick Start Guide
Prerequisites
Make sure you have Python 3.10+ and uv installed on your system.

1. Clone the Repository
Bash
git clone [https://github.com/your-username/eon-weather-pipeline.git](https://github.com/your-username/eon-weather-pipeline.git)
cd eon-weather-pipeline
2. Set Up the Environment & Dependencies
Using uv to handle virtual environments and packages lightning-fast:

Bash
# Create a virtual environment
uv venv

# Activate the virtual environment
source .venv/bin/activate  # On Linux/macOS
# .venv\Scripts\activate   # On Windows

# Install required dependencies
uv pip install -r requirements.txt
3. Run the Pipeline
Execute the main orchestrator script:

Python
python main.py
You will see confirmation logs as the data flows successfully through the Bronze, Silver, and Gold layers, ending with the generation of data/analyst_report.md.

📁 Project Structure
Plaintext
eon-weather-pipeline/
├── data/
│   ├── bronze/      # Raw API JSON responses
│   ├── silver/      # Cleaned tabular datasets (.csv)
│   ├── gold/        # Aggregated business metrics (.csv)
│   └── analyst_report.md  # Final generated output report
│
├── main.py          # End-to-end pipeline orchestrator
├── requirements.txt # Project dependencies (requests, pandas, tabulate)
└── README.md        # Project documentation