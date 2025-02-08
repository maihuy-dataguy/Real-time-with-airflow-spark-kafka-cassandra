import os
import sys
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from kafka import KafkaProducer
import json


def fetch_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent=3))
    return res


def transform_data(res):
    data = {}
    location = res['location']
    # data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


def load_streaming_data():
    current_time = time.time()
    producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)

    while True:
        if time.time() > current_time+60:
            break
        try:
            fetched_data = fetch_data()
            transformed_data = transform_data(fetched_data)
            producer.send('user_created', json.dumps(transformed_data).encode('utf-8'))
        except Exception as e:
            print(e)
            continue


default_args = {
    'owner': 'huym',
    'start_date': datetime(2025, 2, 3, 17, 00)
}

dag = DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

streaming_kafka_task = PythonOperator(
    task_id="streaming_data_from_api",
    python_callable=load_streaming_data
)
