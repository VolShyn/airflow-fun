from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from common import etl
from common.assets import raw_lviv
from common.config import CITIES, CROSS_DAG_CITY

_city = next(c for c in CITIES if c["name"] == CROSS_DAG_CITY)

# ingestion side of the cross-dag split: extract + store raw to external storage.
# the extract task declares raw_lviv as an outlet, so completing it produces the
# asset and triggers weather_processing_dag (data-aware scheduling).
with DAG(
    dag_id="weather_ingestion_dag",
    schedule="@daily",
    start_date=datetime(2026, 3, 22),
    catchup=False,
    default_args={"retries": 2},
    params={
        "city": Param(_city["name"], type="string"),
        "lat": Param(_city["lat"], type="number"),
        "lon": Param(_city["lon"], type="number"),
    },
    tags=["weather", "cross-dag"],
) as dag:
    extract_raw = PythonOperator(
        task_id="extract_raw",
        python_callable=etl.extract,
        op_kwargs={
            "city": "{{ params.city }}",
            "lat": "{{ params.lat }}",
            "lon": "{{ params.lon }}",
            "ds": "{{ ds }}",
        },
        outlets=[raw_lviv],
    )
