from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
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
        task_id='extract_anki_data',
        python_callable=pipeline
    )

    task2 = DockerOperator(
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts =[
            Mount(
                source='/root/repos/anki_etl_project/dbt/my_project',
                target='/usr/app',
                type='bind'),
            Mount(source='/root/repos/anki_etl_project/dbt',
                target='/root/.dbt',
                type='bind')
        ],
        network_mode='anki_etl_project_my_network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    task1 >> task2

