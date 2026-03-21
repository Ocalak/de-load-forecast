import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)
df = pd.read_sql("SELECT * FROM hourly_demand_data LIMIT 5", conn)
print("hourly_demand_data columns:", df.columns.tolist())
try:
    df_merged = pd.read_sql("SELECT * FROM merged_demand_data LIMIT 5", conn)
    print("merged_demand_data columns:", df_merged.columns.tolist())
except Exception as e:
    print("merged_demand_data error:", e)

df_weather = pd.read_sql("SELECT * FROM weather_temperature_hourly LIMIT 5", conn)
print("weather_temperature_hourly columns:", df_weather.columns.tolist())
