import psycopg2, pandas as pd, os
from dotenv import load_dotenv
load_dotenv()
try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "postgres"), user=os.getenv("DB_USER", "postgres"), password=os.getenv("DB_PASSWORD", "postgres")
    )
    weather_query = """
    SELECT timestamp, 
           (berlin_temp + hamburg_temp + munich_temp + cologne_temp + 
            frankfurt_temp + stuttgart_temp + duesseldorf_temp + 
            leipzig_temp + dortmund_temp + essen_temp) / 10.0 AS avg_temp
    FROM weather_temperature_hourly
    ORDER BY timestamp DESC LIMIT 144
    """
    df = pd.read_sql(weather_query, conn)
    print("SQL SUCCESS!")
    print(df.head())
    
    weathers = [{"time": str(row["timestamp"]), "value": row["avg_temp"]} for _, row in df.iterrows()]
    import json
    json.dumps(weathers)
    print("JSON DUMP SUCCESS!")
except Exception as e:
    import traceback
    traceback.print_exc()
