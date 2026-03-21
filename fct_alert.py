import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)

query = """
SELECT timestampx, region, error_pct
FROM fct_forecast_monitoring
WHERE anomaly = TRUE
"""
df = pd.read_sql(query, conn)

cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS fct_alerts (
    alert_id SERIAL PRIMARY KEY,
    timestampx TIMESTAMPTZ,
    region TEXT,
    deviation DOUBLE PRECISION,
    severity TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (timestampx, region)
);
""")
conn.commit()

if df.empty:
    print("No anomalies detected. No alerts to generate.")
else:
    def get_severity(error_pct):
        if error_pct > 0.10:
            return "high"
        else:
            return "medium"

    df["deviation"] = df["error_pct"]
    df["severity"] = df["error_pct"].apply(get_severity)

    insert_query = """
    INSERT INTO fct_alerts (timestampx, region, deviation, severity)
    VALUES %s
    ON CONFLICT (timestampx, region) DO NOTHING
    """
    
    rows = df[["timestampx", "region", "deviation", "severity"]].values.tolist()
    
    execute_values(cursor, insert_query, rows)
    inserted = cursor.rowcount
    conn.commit()

    print(f"Generated {inserted} new alerts in fct_alerts.")

    cursor.close()

conn.close()