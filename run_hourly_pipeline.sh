#!/bin/bash
# run_hourly_pipeline.sh
# This script runs the lightweight hourly updates without retraining the global forecast model
echo "Starting HOURLY pipeline at $(date)"
python3 01dataentso.py
python3 03avg_demad.py
python3 build_monitoring.py
python3 fct_alert.py
python3 hourly_agent.py
echo "Hourly pipeline complete."
