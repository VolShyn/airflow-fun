from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

from common import etl
from common.assets import raw_lviv
from common.config import CROSS_DAG_CITY

# processing side of the cross-dag split: scheduled on the raw_lviv asset, so it
# runs whenever weather_ingestion_dag produces fresh raw data. reads raw from
# external storage, transforms, runs data quality, writes the final dataset.
with DAG(
    dag_id="weather_processing_dag",
    schedule=[raw_lviv],
    start_date=datetime(2026, 3, 22),
    catchup=False,
    default_args={"retries": 2},
    params={"city": Param(CROSS_DAG_CITY, type="string")},
    tags=["weather", "cross-dag"],
) as dag:
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
    transform >> quality_check >> load
