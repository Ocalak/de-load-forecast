import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta, timezone

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

API_KEY = os.getenv("ENTSOE_API_KEY")
AREA_CODE = "10Y1001A1001A83F"   # Germany-Luxembourg example
BASE_URL = "https://web-api.tp.entsoe.eu/api"

NS = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}


def to_entsoe_str(dt: datetime) -> str:
    """Convert aware datetime to ENTSO-E UTC format YYYYMMDDHH00."""
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y%m%d%H00")


def month_ranges(start_dt: datetime, end_dt: datetime):
    """Yield month-sized [start, end) chunks."""
    current = start_dt
    while current < end_dt:
        if current.month == 12:
            nxt = current.replace(year=current.year + 1, month=1, day=1)
        else:
            nxt = current.replace(month=current.month + 1, day=1)
        yield current, min(nxt, end_dt)
        current = nxt


def fetch_xml(period_start: datetime, period_end: datetime) -> str:
    params = {
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": AREA_CODE,
        "periodStart": to_entsoe_str(period_start),
        "periodEnd": to_entsoe_str(period_end),
        "securityToken": API_KEY,
    }

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.text


def parse_load_xml(xml_text: str, area_code: str):
    root = ET.fromstring(xml_text)
    rows = []

    for ts in root.findall(".//ns:TimeSeries", NS):
        period = ts.find("ns:Period", NS)
        if period is None:
            continue

        start_node = period.find("ns:timeInterval/ns:start", NS)
        resolution_node = period.find("ns:resolution", NS)

        if start_node is None or resolution_node is None:
            continue

        start_str = start_node.text
        resolution = resolution_node.text
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

        res_map = {"PT15M": 15, "PT30M": 30, "PT60M": 60}
        minutes = res_map.get(resolution, 60)

        for pt in period.findall("ns:Point", NS):
            pos_node = pt.find("ns:position", NS)
            qty_node = pt.find("ns:quantity", NS)

            if pos_node is None or qty_node is None:
                continue

            position = int(pos_node.text)
            quantity = float(qty_node.text)
            timestamp = start_dt + timedelta(minutes=minutes * (position - 1))

            rows.append((
                timestamp,      # ts_utc
                area_code,      # area_code
                resolution,     # resolution
                position,       # position
                quantity        # quantity_mw
            ))

    return rows


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS entsoe_load (
                ts_utc        TIMESTAMPTZ PRIMARY KEY,
                area_code     TEXT NOT NULL,
                resolution    TEXT NOT NULL,
                position      INTEGER NOT NULL,
                quantity_mw   DOUBLE PRECISION NOT NULL,
                inserted_at   TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()


def upsert_rows(conn, rows):
    if not rows:
        return

    sql = """
        INSERT INTO entsoe_load (
            ts_utc, area_code, resolution, position, quantity_mw
        )
        VALUES %s
        ON CONFLICT (ts_utc) DO UPDATE
        SET area_code   = EXCLUDED.area_code,
            resolution  = EXCLUDED.resolution,
            position    = EXCLUDED.position,
            quantity_mw = EXCLUDED.quantity_mw;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def main():
    start_dt = datetime(2023, 3, 17, 0, 0, tzinfo=timezone.utc)
    end_dt   = datetime.now(timezone.utc)

    conn = psycopg2.connect(**DB_CONFIG)
    create_table(conn)

    total_rows = 0

    try:
        for chunk_start, chunk_end in month_ranges(start_dt, end_dt):
            print(f"Fetching {chunk_start} -> {chunk_end}")
            xml_text = fetch_xml(chunk_start, chunk_end)
            rows = parse_load_xml(xml_text, AREA_CODE)
            print(f"Parsed {len(rows)} rows")
            upsert_rows(conn, rows)
            total_rows += len(rows)

        print(f"Done. Total parsed rows: {total_rows}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()