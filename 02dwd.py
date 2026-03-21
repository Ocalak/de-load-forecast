import os
import logging
from datetime import datetime, timezone
from typing import List, Dict
import re
from datetime import datetime,timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

import openmeteo_requests
import requests_cache
from retry_requests import retry

load_dotenv()

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# DB connection
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# -----------------------------
# Time range
# Same period as your load demand backfill
# -----------------------------
START_DATE = "2023-03-17"
END_DATE = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")

# -----------------------------
# Cities
# -----------------------------
CITIES: List[Dict] = [
    {"city": "Berlin", "latitude": 52.5200, "longitude": 13.4050},
    {"city": "Hamburg", "latitude": 53.5500, "longitude": 10.0000},
    {"city": "Munich", "latitude": 48.1375, "longitude": 11.5755},
    {"city": "Cologne", "latitude": 50.9364, "longitude": 6.9528},
    {"city": "Frankfurt am Main", "latitude": 50.1106, "longitude": 8.6822},
    {"city": "Stuttgart", "latitude": 48.7775, "longitude": 9.1800},
    {"city": "Düsseldorf", "latitude": 51.2217, "longitude": 6.7762},
    {"city": "Dortmund", "latitude": 51.5136, "longitude": 7.4653},
    {"city": "Essen", "latitude": 51.4583, "longitude": 7.0158},
    {"city": "Leipzig", "latitude": 51.3400, "longitude": 12.3750},
]

# -----------------------------
# Open-Meteo client
# Historical API endpoint
# -----------------------------
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def create_temperature_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS weather_temperature_hourly (
        ts_utc            TIMESTAMPTZ PRIMARY KEY,
        country_code      TEXT NOT NULL DEFAULT 'DE',
        avg_temperature_c DOUBLE PRECISION NOT NULL,
        city_count        INTEGER NOT NULL,
        source            TEXT NOT NULL DEFAULT 'OPEN_METEO',
        inserted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fetch_city_temperature(city: str, latitude: float, longitude: float,
                           start_date: str, end_date: str) -> pd.DataFrame:
    logger.info("Fetching temperature for %s", city)

    # Open-Meteo's archive has a 5-day delay for reliable data archiving.
    split_date_dt = datetime.now(timezone.utc) - timedelta(days=5)
    split_date_str = split_date_dt.strftime("%Y-%m-%d")
    forecast_start_date_str = (split_date_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    frames = []

    # 1. Fetch from Archive API
    try:
        if start_date <= split_date_str:
            params_archive = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date,
                "end_date": split_date_str,
                "hourly": "temperature_2m",
                "timezone": "UTC",
            }
            res_archive = openmeteo.weather_api(ARCHIVE_URL, params=params_archive)[0]
            hourly_archive = res_archive.Hourly()
            df_archive = pd.DataFrame({
                "ts_utc": pd.date_range(
                    start=pd.to_datetime(hourly_archive.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly_archive.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly_archive.Interval()),
                    inclusive="left"
                ),
                "temperature_c": hourly_archive.Variables(0).ValuesAsNumpy(),
            })
            frames.append(df_archive)
    except Exception as e:
        logger.warning("Archive fetch failed for %s: %s", city, e)

    # 2. Fetch from Forecast API
    try:
        # If start_date is more recent than our split, only use start_date
        actual_forecast_start = min(forecast_start_date_str, start_date) if split_date_str < start_date else forecast_start_date_str
        
        params_forecast = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": actual_forecast_start,
            "end_date": end_date,
            "hourly": "temperature_2m",
            "timezone": "UTC",
        }
        res_forecast = openmeteo.weather_api(FORECAST_URL, params=params_forecast)[0]
        hourly_forecast = res_forecast.Hourly()
        df_forecast = pd.DataFrame({
            "ts_utc": pd.date_range(
                start=pd.to_datetime(hourly_forecast.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly_forecast.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly_forecast.Interval()),
                inclusive="left"
            ),
            "temperature_c": hourly_forecast.Variables(0).ValuesAsNumpy(),
        })
        frames.append(df_forecast)
    except Exception as e:
        logger.warning("Forecast fetch failed for %s: %s", city, e)

    if not frames:
        raise ValueError(f"Could not fetch any data for {city}")

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["ts_utc"]).sort_values("ts_utc").reset_index(drop=True)
    df["city"] = city
    return df


def build_average_temperature_dataset(start_date: str, end_date: str) -> pd.DataFrame:
    city_frames = []

    for city_info in CITIES:
        df_city = fetch_city_temperature(
            city=city_info["city"],
            latitude=city_info["latitude"],
            longitude=city_info["longitude"],
            start_date=start_date,
            end_date=end_date,
        )
        city_frames.append(df_city)

    df_all = pd.concat(city_frames, ignore_index=True)

    # Average temperature across cities by hour
    df_avg = (
        df_all.groupby("ts_utc", as_index=False)
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            city_count=("city", "nunique")
        )
        .sort_values("ts_utc")
        .reset_index(drop=True)
    )

    return df_avg


def upsert_temperature_data(conn, df_avg: pd.DataFrame):
    rows = [
        (
            row.ts_utc.to_pydatetime(),
            "DE",
            float(row.avg_temperature_c),
            int(row.city_count),
        )
        for row in df_avg.itertuples(index=False)
    ]

    sql = """
    INSERT INTO weather_temperature_hourly (
        ts_utc, country_code, avg_temperature_c, city_count
    )
    VALUES %s
    ON CONFLICT (ts_utc) DO UPDATE
    SET avg_temperature_c = EXCLUDED.avg_temperature_c,
        city_count = EXCLUDED.city_count,
        updated_at = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        create_temperature_table(conn)

        df_avg = build_average_temperature_dataset(
            start_date=START_DATE,
            end_date=END_DATE
        )

        logger.info("Averaged temperature rows: %s", len(df_avg))
        logger.info("Sample:\n%s", df_avg.head())

        upsert_temperature_data(conn, df_avg)

        logger.info("Temperature data successfully saved to PostgreSQL.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()