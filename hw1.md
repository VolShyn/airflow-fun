# HW1 — reproduce

`weather.db` is shipped in the repo. From the repo root:

Set `DB_PATH` in `weather_dag.py` to the absolute path of the bundled `weather.db`.

Drop `first_dag.py` and `weather_dag.py` into your `$AIRFLOW_HOME/dags/`.

Register connections and the API key:

```
airflow connections add weather_conn \
    --conn-type sqlite --conn-host $(pwd)/weather.db
airflow connections add openweather_conn \
    --conn-type http --conn-host https://api.openweathermap.org
airflow variables set WEATHER_API_KEY *APIKEY*
```

Run:

```
airflow dags test weather_dag 2026-03-22
```

Expect 5 new rows:

```
sqlite3 weather.db \
    "SELECT city, temp, wind_speed FROM measures ORDER BY timestamp DESC LIMIT 10;"
```
