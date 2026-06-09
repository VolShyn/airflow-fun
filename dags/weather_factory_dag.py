from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from common import etl
from common.config import CITIES, CROSS_DAG_CITY, WIND_ALERT_THRESHOLD


# factory: build one full-etl dag for a single city.
# all hardcoded values are exposed as dag params and pushed into the tasks
# via jinja templates ({{ params.* }}, {{ ds }}), so a run can be re-parametrized
# from the ui / cli without touching code.
def build_weather_dag(city):
    with DAG(
        dag_id=f"weather_{city['name'].lower()}",
        schedule="@daily",
        start_date=datetime(2026, 3, 22),
        catchup=False,
        default_args={"retries": 2},
        params={
            "city": Param(city["name"], type="string"),
            "lat": Param(city["lat"], type="number"),
            "lon": Param(city["lon"], type="number"),
            "wind_alert_threshold": Param(WIND_ALERT_THRESHOLD, type="number"),
        },
        tags=["weather", "factory"],
    ) as dag:
        extract = PythonOperator(
            task_id="extract",
            python_callable=etl.extract,
            op_kwargs={
                "city": "{{ params.city }}",
                "lat": "{{ params.lat }}",
                "lon": "{{ params.lon }}",
                "ds": "{{ ds }}",
            },
        )
        transform = PythonOperator(
            task_id="transform",
            python_callable=etl.transform,
            op_kwargs={"city": "{{ params.city }}", "ds": "{{ ds }}"},
        )
        quality_check = PythonOperator(
            task_id="quality_check",
            python_callable=etl.quality_check,
            op_kwargs={"city": "{{ params.city }}", "ds": "{{ ds }}"},
        )
        load = PythonOperator(
            task_id="load",
            python_callable=etl.load,
            op_kwargs={"city": "{{ params.city }}", "ds": "{{ ds }}"},
        )
        extract >> transform >> quality_check >> load
    return dag


# generate a dag per city, except the one handled by the cross-dag split
for _c in CITIES:
    if _c["name"] == CROSS_DAG_CITY:
        continue
    globals()[f"weather_{_c['name'].lower()}"] = build_weather_dag(_c)
