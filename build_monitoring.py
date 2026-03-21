import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

# actuals
actuals_query = """
SELECT timestamp as timestampx, 'DE' as region, demand_mw as actual_demand
FROM hourly_demand_data
"""

# forecasts
forecast_query = """
SELECT forecast_target_time as timestampx, region, predicted_demand as forecast_demand
FROM fct_demand_forecast
"""

actuals = pd.read_sql(actuals_query, conn)
forecasts = pd.read_sql(forecast_query, conn)

actuals["timestampx"] = pd.to_datetime(actuals["timestampx"], utc=True)
forecasts["timestampx"] = pd.to_datetime(forecasts["timestampx"], utc=True)

# merge actuals and forecasts
df = pd.merge(actuals, forecasts, on=["timestampx", "region"], how="inner")

cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS fct_forecast_monitoring (
    timestampx TIMESTAMPTZ,
    region TEXT,
    actual_demand DOUBLE PRECISION,
    forecast_demand DOUBLE PRECISION,
    error_pct DOUBLE PRECISION,
    anomaly BOOLEAN,
    PRIMARY KEY (timestampx, region)
);
""")
conn.commit()

if df.empty:
    print("No matching actuals and forecasts found to monitor. Run kaggle.py so predictions populate into history.")
else:
    # calculations
    df["error_pct"] = ((df["actual_demand"] - df["forecast_demand"]).abs() / df["forecast_demand"]).round(4)
    df["anomaly"] = df["error_pct"] > 0.000001  # Highly sensitive threshold for testing

    insert_query = """
    INSERT INTO fct_forecast_monitoring
    (timestampx, region, actual_demand, forecast_demand, error_pct, anomaly)
    VALUES %s
    ON CONFLICT (timestampx, region) DO UPDATE 
    SET actual_demand=EXCLUDED.actual_demand, forecast_demand=EXCLUDED.forecast_demand, 
        error_pct=EXCLUDED.error_pct, anomaly=EXCLUDED.anomaly;
    """

    rows = df[["timestampx", "region", "actual_demand", "forecast_demand", "error_pct", "anomaly"]].values.tolist()

    execute_values(cursor, insert_query, rows)
    conn.commit()

    print(f"Inserted/Updated {len(rows)} monitored hours into fct_forecast_monitoring.")
    
    anomalies = df[df["anomaly"] == True]
    print(f"Detected {len(anomalies)} anomalies in current monitoring window.")

    cursor.close()

conn.close()