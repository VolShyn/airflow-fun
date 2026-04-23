# HW2 — reproduce

Same DAG as HW1, now with TaskGroups per city (`extract → transform → branch → {normal_load | alert_and_load}`), XCom-passed payloads, wind-threshold branching, retries in `default_args`, and Celery as executor.

## Setup

Postgres and Redis must be running. Create the metadata and weather databases:

```
createdb airflow
createdb weather
```

Create the `measures` table in the weather db:

```
psql weather -c "
CREATE TABLE IF NOT EXISTS measures (
    timestamp  BIGINT,
    city       TEXT,
    temp       FLOAT,
    humidity   FLOAT,
    cloudiness FLOAT,
    wind_speed FLOAT
);"
```

Install the extras:

```
pip install "apache-airflow-providers-celery>=3.3.0" apache-airflow-providers-postgres
```

`airflow.cfg` is already configured for Celery + Postgres + Redis. Init the metadata DB:

```
airflow db migrate
```

Register connections and variable:

```
airflow connections add weather_conn \
    --conn-type postgres --conn-host localhost --conn-schema weather --conn-login <os_user>
airflow connections add openweather_conn \
    --conn-type http --conn-host https://api.openweathermap.org
airflow variables set WEATHER_API_KEY *APIKEY*
```

Copy the DAG:

```
cp weather_dag.py $AIRFLOW_HOME/dags/
```

## Start the stack

Four separate shells:

```
airflow scheduler
airflow api-server
airflow celery worker
airflow celery flower
```

Flower UI at `http://localhost:5555` — screenshot is in `flower.png`.

UI login credentials are in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`.

## Verify

```
airflow dags test weather_dag 2026-03-22
```

Check that exactly one load branch ran per city (the other is skipped):

```
psql weather -c "SELECT city, wind_speed FROM measures ORDER BY timestamp DESC LIMIT 5;"
```

To force the alert branch, set `WIND_ALERT_THRESHOLD = 0.0` in `weather_dag.py`.
