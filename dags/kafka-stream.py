from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar Steve',
    'start_date': datetime(2024, 7, 25)
}