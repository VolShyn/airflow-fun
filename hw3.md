# HW3

In HW2 I had a single `weather_dag` that wrote all five cities into Postgres and ran on Celery. For HW3 I broke that apart and moved the storage to plain files on disk.

What changed:

I stopped hardcoding the city, coordinates and thresholds inside the DAG. They are DAG params now, and every task gets them through Jinja, e.g. `op_kwargs={"city": "{{ params.city }}", "ds": "{{ ds }}"}`. So I can re-run a DAG for a different city or date from the UI without editing code.

I split the Lviv pipeline into two DAGs. `weather_ingestion_dag` extracts and stores the raw data, and `weather_processing_dag` reads that raw data, transforms it, runs the quality checks and writes the final dataset. I connected them with an Airflow 3 Asset instead of a sensor: the ingestion task declares `outlets=[raw_lviv]`, the processing DAG has `schedule=[raw_lviv]`, so producing the raw data is what triggers processing. No polling.

For the other four cities I wrote a factory. `build_weather_dag(city)` in `weather_factory_dag.py` builds a full extract→transform→quality→load DAG, and I call it in a loop, so `weather_kyiv`, `weather_kharkiv`, `weather_odesa` and `weather_zhmerynka` all come from one function. The actual step logic lives once in `dags/common/etl.py` and is shared by the factory and the split DAGs.

Why files and not Postgres: I started toward Postgres because that's what HW2 used, but the rubric wants each step to store its output and a re-run to resume from the failed step without redoing finished steps. With files that falls out for free. Each step writes `DATA_DIR/<stage>/<city>/<ds>.json`, and starts with a guard `if storage.stage_exists(stage, city, ds): return`. Because the path is deterministic per city and date, a re-run sees the finished stages already on disk and skips them, so it only redoes the failed step onward and never calls the API twice. With Postgres I'd have to write extra idempotency/upsert logic to get the same thing, and the files are easy to just open and check. So I dropped Postgres for this one; `weather_conn` isn't needed anymore.

The quality check (`etl.quality_check`) just reads the transformed record and asserts plausible ranges (temp present, humidity 0–100, wind ≥ 0). A bad value fails the task and leaves the earlier stages on disk, which is exactly what lets the resume work.

## Reproduce

```
export AIRFLOW_HOME=<your airflow home>
export WEATHER_DATA_DIR=/tmp/weather_hw3     # storage root, optional
cp -r dags/* $AIRFLOW_HOME/dags/             # copy DAGs and common/ together

airflow connections add openweather_conn \
    --conn-type http --conn-host https://api.openweathermap.org
airflow variables set WEATHER_API_KEY *APIKEY*
```

One factory city:

```
airflow dags test weather_kyiv 2026-03-22
ls /tmp/weather_hw3/{raw,transformed,final}/kyiv/
```

The cross-dag pair (under `dags test` run both with the same date; on a live scheduler the asset triggers processing on its own):

```
airflow dags test weather_ingestion_dag 2026-03-22
airflow dags test weather_processing_dag 2026-03-22
cat /tmp/weather_hw3/final/lviv/2026-03-22.json
```
