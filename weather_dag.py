import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# above this wind speed (m/s) the branch routes to the alert path
WIND_ALERT_THRESHOLD = 10.0

# first city is also used by the sensor to check api availability
CITIES = [
    {"name": "Lviv", "lat": 49.8397, "lon": 24.0297},
    {"name": "Kyiv", "lat": 50.4501, "lon": 30.5234},
    {"name": "Kharkiv", "lat": 49.9935, "lon": 36.2304},
    {"name": "Odesa", "lat": 46.4825, "lon": 30.7233},
    {"name": "Zhmerynka", "lat": 49.0391, "lon": 28.1112},
]


def _extract(lat, lon, ds, **_):
    # fetch raw weather for one city, returned value is pushed to xcom
    api_key = Variable.get("WEATHER_API_KEY")
    # ds is the execution date YYYY-MM-DD, unix dt makes backfills historical
    dt = int(datetime.strptime(ds, "%Y-%m-%d").timestamp())
    resp = requests.get(
        "https://api.openweathermap.org/data/3.0/onecall/timemachine",
        params={"lat": lat, "lon": lon, "dt": dt, "appid": api_key},
        timeout=10,
    )
    resp.raise_for_status()
    # timemachine returns a "data" array, we take the first (and only) element
    return resp.json()["data"][0]


def _transform(city, group_id, ti, **_):
    # pull raw from extract via xcom, shape the row we want to store
    raw = ti.xcom_pull(task_ids=f"{group_id}.extract")
    return {
        "timestamp": raw["dt"],
        "city": city,
        "temp": raw["temp"],
        "humidity": raw["humidity"],
        "cloudiness": raw["clouds"],
        "wind_speed": raw["wind_speed"],
    }


def _branch(group_id, ti, **_):
    # route on the transformed wind speed to the matching load task
    record = ti.xcom_pull(task_ids=f"{group_id}.transform")
    if record["wind_speed"] > WIND_ALERT_THRESHOLD:
        return f"{group_id}.alert_load"
    return f"{group_id}.normal_load"


def _load(group_id, alert, ti, **_):
    # pull transformed row from xcom and insert; alert path also logs a warning
    record = ti.xcom_pull(task_ids=f"{group_id}.transform")
    if alert:
        log.warning(
            "wind alert for %s: %.1f m/s exceeds %.1f",
            record["city"],
            record["wind_speed"],
            WIND_ALERT_THRESHOLD,
        )
    hook = PostgresHook(postgres_conn_id="weather_conn")
    hook.run(
        "INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed) VALUES (%s, %s, %s, %s, %s, %s)",
        parameters=(
            record["timestamp"],
            record["city"],
            record["temp"],
            record["humidity"],
            record["cloudiness"],
            record["wind_speed"],
        ),
    )


with DAG(
    dag_id="weather_dag",
    schedule="@daily",
    start_date=datetime(2026, 3, 22),
    catchup=False,
    # retry logic applied to every task in the dag
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
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

    # one task group per city: extract -> transform -> branch -> {normal,alert} load
    for city in CITIES:
        group_id = city["name"].lower()
        with TaskGroup(group_id=group_id) as tg:
            extract = PythonOperator(
                task_id="extract",
                python_callable=_extract,
                op_kwargs={"lat": city["lat"], "lon": city["lon"]},
            )
            transform = PythonOperator(
                task_id="transform",
                python_callable=_transform,
                op_kwargs={"city": city["name"], "group_id": group_id},
            )
            branch = BranchPythonOperator(
                task_id="branch",
                python_callable=_branch,
                op_kwargs={"group_id": group_id},
            )
            normal_load = PythonOperator(
                task_id="normal_load",
                python_callable=_load,
                op_kwargs={"group_id": group_id, "alert": False},
            )
            alert_load = PythonOperator(
                task_id="alert_load",
                python_callable=_load,
                op_kwargs={"group_id": group_id, "alert": True},
            )
            extract >> transform >> branch >> [normal_load, alert_load]

        # b_create and check_api are independent, both must finish before any city
        [b_create, check_api] >> tg
