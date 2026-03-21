import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)

actuals = pd.read_sql("SELECT timestamp as timestampx, 'DE' as region, demand_mw as actual_demand FROM hourly_demand_data ORDER BY timestamp DESC LIMIT 5", conn)
forecasts = pd.read_sql("SELECT forecast_target_time as timestampx, region, predicted_demand as forecast_demand FROM fct_demand_forecast ORDER BY forecast_target_time ASC LIMIT 5", conn)

actuals["timestampx"] = pd.to_datetime(actuals["timestampx"], utc=True)
forecasts["timestampx"] = pd.to_datetime(forecasts["timestampx"], utc=True)

print("Actuals:")
print(actuals)
print("\nForecasts:")
print(forecasts)

df = pd.merge(actuals, forecasts, on=["timestampx", "region"], how="inner")
print(f"\nMerged length: {len(df)}")
