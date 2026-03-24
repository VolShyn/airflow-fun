from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="first_dag",
    schedule="@daily",
    # fixed date required — datetime.now() causes unpredictable backfills on scheduler restart
    start_date=datetime(2026, 3, 22),
) as dag:
    b = BashOperator(
        task_id="simple_command",
        bash_command="echo 'Hello World!'",
    )

