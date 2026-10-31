#!/bin/bash
# run_daily_pipeline.sh
# Add to crontab via: 0 10 * * * /path/to/de-load-forecast/run_daily_pipeline.sh

cd "$(dirname "$0")"

echo "=================================="
echo "Starting daily pipeline at $(date)"
echo "=================================="
echo "Running ENTSO-E fetch..."
python3 01dataentso.py
echo "Running DWD Weather fetch..."
python3 02dwd.py
echo "Averaging demand data..."
python3 03avg_demad.py
echo "Running Kaggle Forecast..."
python3 kaggle.py
echo "Building forecast monitors..."
python3 build_monitoring.py
echo "Evaluating & Triggering Alerts..."
python3 fct_alert.py
echo "Running AI Agent Alert Dispatcher..."
python3 agent.py
echo "Daily pipeline complete at $(date)."
echo "=================================="
