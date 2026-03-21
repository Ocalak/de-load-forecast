import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)   

cursor = conn.cursor()

query = """
SELECT *
FROM merged_demand_data
ORDER BY timestamp
"""

df = pd.read_sql(query, conn)
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df['hour'] = df['timestamp'].dt.hour
df['dow'] = df['timestamp'].dt.dayofweek


df['season'] = df['timestamp'].dt.month
def get_season(month):
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'fall'
df['season'] = df['season'].apply(get_season)
df['lag_24'] = df['demand_mw'].shift(24)
df['lag_48'] = df['demand_mw'].shift(48)
df['lag_72'] = df['demand_mw'].shift(72)
df['lag_168'] = df['demand_mw'].shift(168)
df['t_lag1'] = df['temp'].shift(1)
df['t_lag2'] = df['temp'].shift(2)
df['t_lag3'] = df['temp'].shift(3)
df['t_lag4'] = df['temp'].shift(4)
df['t_lag5'] = df['temp'].shift(5)
df['t_lag6'] = df['temp'].shift(6)
df['t_lag24'] = df['temp'].shift(24)

df["rolling_24"] = df["demand_mw"].rolling(24).mean()
df["rolling_48"] = df["demand_mw"].rolling(48).mean()

create_table_query = """
DROP TABLE IF EXISTS fct_energy_features;

CREATE TABLE fct_energy_features (
    timestamp TIMESTAMP,
    demand_mw DOUBLE PRECISION,
    hour INT,
    dow INT,
    lag_24 DOUBLE PRECISION,
    lag_48 DOUBLE PRECISION,
    lag_72 DOUBLE PRECISION,
    lag_168 DOUBLE PRECISION,
    season TEXT,
    temp DOUBLE PRECISION,
    t_lag1 DOUBLE PRECISION,
    t_lag2 DOUBLE PRECISION,
    t_lag3 DOUBLE PRECISION,
    t_lag4 DOUBLE PRECISION,
    t_lag5 DOUBLE PRECISION,
    t_lag6 DOUBLE PRECISION,
    t_lag24 DOUBLE PRECISION,
    rolling_24 DOUBLE PRECISION,
    rolling_48 DOUBLE PRECISION
);
"""


insert_query ="""
INSERT INTO fct_energy_features
(timestamp, demand_mw, hour, dow, lag_24, lag_48, lag_72, lag_168, season, temp, t_lag1, t_lag2, t_lag3, t_lag4, t_lag5, t_lag6, t_lag24, rolling_24, rolling_48)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
rows= df[["timestamp", "demand_mw", "hour", "dow", "lag_24", "lag_48", "lag_72", "lag_168", "season", "temp", "t_lag1", "t_lag2", "t_lag3", "t_lag4", "t_lag5", "t_lag6", "t_lag24", "rolling_24", "rolling_48"]].dropna().values.tolist()
cursor.execute(create_table_query)

cursor.executemany(insert_query, rows)

conn.commit()
print(f"Inserted {len(rows)} rows into fct_energy_features table.")
cursor.close()
conn.close()