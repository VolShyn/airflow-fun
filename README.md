
Fetches weather for Lviv, Kyiv, Kharkiv, Odesa, and Zhmerynka from OpenWeatherMap once a day
and stores it in a local SQLite database. Supports backfilling — if you run it for a past date,
it actually returns the weather for that date, not today's.

Before running anything, set up the connections and the API key:

```
airflow connections add weather_conn --conn-type sqlite --conn-host /path/to/weather.db
airflow connections add openweather_conn --conn-type http --conn-host https://api.openweathermap.org
airflow variables set WEATHER_API_KEY <your_api_key>
```

The pipeline has three tasks. Table creation and API health check run in parallel — they don't
depend on each other so there's no point waiting. Once both are done, a single Python task
fetches data for all five cities and writes it to the DB.

The fetching and storing used to be split into separate tasks with XCom passing data between
them. That meant interpolating values into a raw SQL string, which is just SQL injection waiting
to happen. Now it's one function with parameterized queries.

Historical data works via the timemachine endpoint. Airflow passes `ds` — the execution date as
YYYY-MM-DD — into the callable, which gets converted to a Unix timestamp and sent as `dt` in the
request. So backfilling some `2023-03-16` actually asks OpenWeatherMap for `2023-03-16`, not right now.

`start_date` is a fixed date. Using `datetime.now()` shifts the backfill window every time the
scheduler restarts, which leads to duplicate or missing runs. `catchup=False` means it won't
try to backfill everything from start_date to today on first deploy.
