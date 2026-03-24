import json
import sqlite3
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor

DB_PATH = "PLACEHOLDER"

# random working coordinates
LAT = 33.44
LON = -94.04


def _process_and_inject(ti):
    # pull json parsed by response_filter in extract_data
    info = ti.xcom_pull("extract_data")
    # onecall 3.0 returns current conditions under "current" key
    current = info["current"]
    row = (
        current["dt"],  # unix timestamp
        current["temp"],  # kelvin by default
        current["humidity"],  # percent
        current["clouds"],  # cloudiness percent
        current["wind_speed"],  # m/s
    )
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO measures (timestamp, temp, humidity, cloudiness, wind_speed) VALUES (?, ?, ?, ?, ?)",
            row,
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
                temp       FLOAT,
                humidity   FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT
            );
        """,
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_conn",
        endpoint="data/3.0/onecall",
        request_params={
            "lat": LAT,
            "lon": LON,
            "appid": Variable.get("WEATHER_API_KEY"),
        },
    )

    extract_data = HttpOperator(
        task_id="extract_data",
        http_conn_id="openweather_conn",
        endpoint="data/3.0/onecall",
        data={"lat": LAT, "lon": LON, "appid": Variable.get("WEATHER_API_KEY")},
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True,
    )

    process_and_inject = PythonOperator(
        task_id="process_and_inject",
        python_callable=_process_and_inject,
    )

    # b_create and check_api are independent so we can run in parallel
    [b_create, check_api] >> extract_data >> process_and_inject
