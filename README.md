
Fetches weather for Lviv from OpenWeatherMap once a day and dumps it into SQLite. 

You need two connections: `weather_conn` (sqlite, pointing at `weather.db`) and `openweather_conn`
(http, pointing at api.openweathermap.org). You also need a WEATHER_API_KEY variable set.
Without these the DAG will fail immediately, so set them up before triggering anything.

Commands needed:

```
airflow connections add weather_conn --conn-type sqlite --conn-host *path-to-db*
airflow connections add openweather_conn --conn-type http --conn-host https://api.openweathermap.org
airflow variables set WEATHER_API_KEY <your_api_key>
```

The pipeline is four tasks. Table creation and API health check run in parallel because there
is no reason they shouldn't. Then extract, then write to DB. Simple.

Process and inject are one task, not two. Splitting them meant passing data through xcom and
then interpolating it into a SQL string — that is just SQL injection waiting to happen. One
Python function with a parameterized query is the right call.

`start_date` is hardcoded. `datetime.now()` as a start date is wrong. Catchup is off so it doesn't try to backfill the entire history on first run.
