import json
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 7, 0, 0, 0)
}

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['postcode'] = res['location']['postcode']
    data['city'] = res['location']['city']
    data['state'] = res['location']['state']
    data['country'] = res['location']['country']
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['cell'] = res['cell']
    data['latitude'] = location['coordinates']['latitude']
    data['longitude'] = location['coordinates']['longitude']
    data['timezone'] = location['timezone']['offset']
    data['utc'] = location['timezone']['description']

    return data

def stream_data():
    from confluent_kafka import Producer

    producer = Producer({'bootstrap.servers': 'broker:29092'})

    current_time = time.time()

    while time.time() < current_time + 120:
        try:
            res = get_data()
            data = format_data(res)
            producer.produce('users', json.dumps(data).encode('utf-8'))
            producer.flush()

        except Exception as e:
            print(e)


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    streaming_task = PythonOperator(
        task_id='streaming_data_from_api',
        python_callable=stream_data
    )
