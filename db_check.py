import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)
cursor = conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
tables = [row[0] for row in cursor.fetchall()]
print("Tables:", tables)

for t in ["raw_energy_demand", "fct_demand_forecast", "entsoe_load", "hourly_demand_data"]:
    if t in tables:
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{t}'")
        cols = cursor.fetchall()
        print(f"Table {t}:", cols)
