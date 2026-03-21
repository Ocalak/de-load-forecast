import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

query = """
SELECT alert_id, timestampx, region, deviation, severity
FROM fct_alerts
ORDER BY alert_id DESC
LIMIT 1
"""

df = pd.read_sql(query, conn)

if df.empty:
    print("No alerts found.")
else:
    row = df.iloc[0]
    message = (
        f"Alert {row['alert_id']}: Region {row['region']} shows a "
        f"{round(row['deviation'] * 100, 2)}% deviation at {row['timestampx']}. "
        f"Severity is {row['severity']}."
    )
    print(message)

conn.close()