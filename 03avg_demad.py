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
create_hourly_table_sql = f"""
DROP TABLE IF EXISTS hourly_demand_data;
CREATE TABLE hourly_demand_data AS
SELECT date_trunc('hour',ts_utc) AS timestamp,
       AVG(quantity_mw) AS demand_mw
       FROM entsoe_load
       GROUP BY date_trunc('hour',ts_utc)
       ORDER BY timestamp;
"""
cursor.execute(create_hourly_table_sql)
conn.commit()


cursor.close()
conn.close()