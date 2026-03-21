import psycopg2
from datetime import datetime
#/usr/local/bin/python3 --version
import pandas as pd 
import numpy as np
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
DROP TABLE IF EXISTS merged_demand_data;

CREATE TABLE merged_demand_data AS
SELECT d.timestamp,round(d.demand_mw::numeric,3) AS demand_mw,COALESCE(round(t.avg_temperature_c::numeric,3)) AS temp
FROM hourly_demand_data d
LEFT JOIN weather_temperature_hourly t
ON d.timestamp= t.ts_utc
ORDER BY d.timestamp;
"""

cursor.execute(query)
conn.commit()


cursor.close()
conn.close()