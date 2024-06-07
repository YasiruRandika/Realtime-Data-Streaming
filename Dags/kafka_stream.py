from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 7, 0, 0, 0)
}

def streaming_data_from_api():
    import  json
    import requests

    res = requests.get('https://randomuser.me/api/')
    print(res.json())

# with DAG('user_automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False
# ) as dag:
#
#     streaming_task = PythonOperator(
#         task_id='streaming_data_from_api',
#         python_callable=streaming_data_from_api
#     )

streaming_data_from_api();