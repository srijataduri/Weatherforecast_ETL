from __future__ import annotations

import os
import json
import csv
from datetime import datetime, timedelta
import urllib.parse
import urllib.request

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_logical_date() -> str:
    """
    Returns the DAG run's logical date as YYYY-MM-DD.
    """
    ctx = get_current_context()
    logical_date = ctx["logical_date"]
    return logical_date.strftime("%Y-%m-%d")


def get_next_day(date_str: str) -> str:
    """
    Returns the next day (YYYY-MM-DD) given a YYYY-MM-DD string.
    """
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return (d + timedelta(days=1)).strftime("%Y-%m-%d")


def save_weather_data(city: str, latitude: float, longitude: float,
                      start_date: str, end_date: str, file_path: str) -> None:
    """
    Fetch daily weather data from Open-Meteo for [start_date, end_date).
    Saves to CSV.
    NOTE: Uses urllib (no 'requests' dependency).
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode",
        "timezone": "auto",
    }

    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"

    with urllib.request.urlopen(full_url, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    prcp = daily.get("precipitation_sum", [])
    wcode = daily.get("weathercode", [])

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "temp_max", "temp_min", "precipitation", "weather_code", "city"])
        for i in range(len(dates)):
            writer.writerow([dates[i], tmax[i], tmin[i], prcp[i], str(wcode[i]), city])


def return_snowflake_conn(conn_id: str):
    """
    Returns a cursor from an Airflow Snowflake connection.
    """
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn.cursor()


def populate_table_from_csv(cur, database: str, schema: str, target_table: str, file_path: str) -> None:
    """
    Load CSV rows into Snowflake using INSERT.
    """
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return

    insert_sql = f"""
        INSERT INTO {database}.{schema}.{target_table}
        (date, temp_max, temp_min, precipitation, weather_code, city)
        VALUES (%(date)s, %(temp_max)s, %(temp_min)s, %(precipitation)s, %(weather_code)s, %(city)s)
    """

    for r in rows:
        cur.execute(insert_sql, r)

with DAG(
    dag_id="weather_ETL_incremental",
    start_date=datetime(2026, 2, 28),
    catchup=False,
    schedule="30 3 * * *",
    max_active_runs=1,
    tags=["ETL"],
) as dag:

    LATITUDE = float(Variable.get("weather_latitude"))
    LONGITUDE = float(Variable.get("weather_longitude"))
    CITY = Variable.get("city", default_var="Seattle")

    DATABASE = Variable.get("snowflake_database", default_var="DEMO_DB")
    SCHEMA = Variable.get("snowflake_schema", default_var="PUBLIC")
    TARGET_TABLE = Variable.get("weather_table", default_var="WEATHER_DAILY")

    SNOWFLAKE_CONN_ID = "snowflake_conn"

    @task
    def extract(city: str, longitude: float, latitude: float) -> str:
        date_to_fetch = get_logical_date()
        next_day = get_next_day(date_to_fetch)

        file_path = f"/tmp/{city}_{date_to_fetch}.csv"
        save_weather_data(city, latitude, longitude, date_to_fetch, next_day, file_path)
        print(f"Saved weather CSV: {file_path}")
        return file_path

    @task
    def load(file_path: str, database: str, schema: str, target_table: str) -> None:
        date_to_fetch = get_logical_date()
        next_day = get_next_day(date_to_fetch)

        print(f"========= Incremental update for {date_to_fetch} =========")
        cur = return_snowflake_conn(SNOWFLAKE_CONN_ID)

        try:
            cur.execute("BEGIN;")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
                    date DATE,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    precipitation FLOAT,
                    weather_code VARCHAR,
                    city VARCHAR
                );
            """)

            cur.execute(f"""
                DELETE FROM {database}.{schema}.{target_table}
                WHERE date >= '{date_to_fetch}' AND date < '{next_day}';
            """)

            populate_table_from_csv(cur, database, schema, target_table, file_path)

            cur.execute("COMMIT;")
            print("COMMIT OK")
        except Exception as e:
            cur.execute("ROLLBACK;")
            print("ROLLBACK due to error:", e)
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    fp = extract(CITY, LONGITUDE, LATITUDE)
    load(fp, DATABASE, SCHEMA, TARGET_TABLE)