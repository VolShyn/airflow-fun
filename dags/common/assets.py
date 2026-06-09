from airflow.sdk import Asset

# the raw-data asset produced by weather_ingestion_dag and consumed
# (data-aware schedule) by weather_processing_dag.
raw_lviv = Asset("weather://raw/lviv")
