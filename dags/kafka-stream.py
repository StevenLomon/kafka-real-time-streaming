import requests, json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# from rich import print

default_args = {
    'owner': 'airscholar Steve',
    'start_date': datetime(2024, 7, 25)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]
    return res

def format_data(response): # Format data for the Kafka queue
    data = {}
    data['first_name'] = response.get('name').get('first')
    data['last_name'] = response.get('name').get('last')
    data['gender'] = response.get('gender')
    data['address'] = response.get('location').get('')
    print(data)

def stream_data(): 
    pass
    

# with DAG(
#     'user_automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False) as dag:
    
#         streaming_task = PythonOperator(
#                 task_id='stream_data_from_api',
#                 python_callable=stream_data
#         )

format_data(get_data())