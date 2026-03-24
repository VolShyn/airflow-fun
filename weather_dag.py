import sqlite3
from datetime import datetime

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor

DB_PATH = "PLACEHOLDER"

# first city is also used by the sensor to check api availability
CITIES = [
    {"name": "Lviv", "lat": 49.8397, "lon": 24.0297},
    {"name": "Kyiv", "lat": 50.4501, "lon": 30.5234},
    {"name": "Kharkiv", "lat": 49.9935, "lon": 36.2304},
    {"name": "Odesa", "lat": 46.4825, "lon": 30.7233},
    {"name": "Zhmerynka", "lat": 49.0391, "lon": 28.1112},
]


def _fetch_and_store(ds, **_):
    api_key = Variable.get("WEATHER_API_KEY")
    # ds is the execution date string YYYY-MM-DD, to convert to unix timestamp
    # this makes historical backfills return data for the correct date
    dt = int(datetime.strptime(ds, "%Y-%m-%d").timestamp())

    with sqlite3.connect(DB_PATH) as conn:
        for city in CITIES:
            resp = requests.get(
                "https://api.openweathermap.org/data/3.0/onecall/timemachine",
                params={
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "dt": dt,
                    "appid": api_key,
                },
                timeout=10,
            )
            resp.raise_for_status()
            # timemachine returns a "data" array, we take the first (and only) element
            data = resp.json()["data"][0]
            conn.execute(
                "INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    data["dt"],
                    city["name"],
                    data["temp"],
                    data["humidity"],
                    data["clouds"],
                    data["wind_speed"],
                ),
            )


with DAG(
    dag_id="weather_dag",
    schedule="@daily",
    start_date=datetime(2026, 3, 22),
    catchup=False,
) as dag:
    b_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS measures (
                timestamp  TIMESTAMP,
                city       TEXT,
                temp       FLOAT,
                humidity   FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT
            );
        """,
    )

    # checks that the api is reachable before fetching
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_conn",
        endpoint="data/3.0/onecall",
        request_params={
            "lat": CITIES[0]["lat"],
            "lon": CITIES[0]["lon"],
            "appid": Variable.get("WEATHER_API_KEY"),
        },
    )

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store",
        python_callable=_fetch_and_store,
    )

    # b_create and check_api are independent so we can run in parallel
    [b_create, check_api] >> fetch_and_store
