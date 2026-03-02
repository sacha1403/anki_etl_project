from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import sys

sys.path.append('/opt/airflow/python_scripts')

from insert_records import pipeline

default_args = {
    'description': 'Orchestrator DAG for weather data project',
    'start_date': datetime(2026, 2, 28),
    'catchup': False,
}

dag = DAG(
    dag_id="anki_data_orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=5)
)

with dag:
    task1 = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline
    )