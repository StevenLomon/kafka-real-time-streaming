import requests, json, time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
# from rich import print

default_args = {
    'owner': 'airscholar Steve',
    'start_date': datetime(2024, 7, 25)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]
    return res

def format_data(response): # Format data for the Kafka queue; streamline our data
    data = {}
    data['first_name'] = response.get('name').get('first')
    data['last_name'] = response.get('name').get('last')
    data['gender'] = response.get('gender')
    location = response.get('location')
    data['address'] = f"{str(location.get('street').get('number'))} {location.get('street').get('name')}"
    data['city'] = location.get('city')
    data['state'] = location.get('state')
    data['country'] = location.get('country')
    data['postcode'] = location.get('postcode')
    data['email'] = response.get('email')
    data['username'] = response.get('login').get('username')
    data['date_of_birth'] = response.get('dob').get('date')
    data['age'] = response.get('dob').get('age')
    data['registered_date'] = response.get('registered').get('date')
    data['phone'] = response.get('phone')
    data['picture'] = response.get('picture').get('medium')
    
    return data

def stream_data(): 
    res = get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=3))

    # Publish and push the data to the Kafka queue (?)
    # We are not yet in the Docker container, hence we use localhost instead of 'broker:29092'
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000) # timeout
    producer.send('users_created', json.dumps(res).encode('utf-8'))
    

# with DAG(
#     'user_automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False) as dag:
    
#         streaming_task = PythonOperator(
#                 task_id='stream_data_from_api',
#                 python_callable=stream_data
#         )

stream_data()