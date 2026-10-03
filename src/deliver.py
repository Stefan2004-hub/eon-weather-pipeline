from datetime import datetime

import pandas as pd


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
