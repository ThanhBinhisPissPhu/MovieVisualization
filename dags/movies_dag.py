from datetime import datetime
from airflow import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
import pandas as pd
import json
import time
import logging
import os
from streamer import JsonStreamerPandas
from kafka import KafkaProducer

movie_path = 'dags/streaming_data/streaming_movies.jsonl'

default_args = {
    'owner': 'thanhbinh',
    'retries': 3,
    'start_date': datetime(2024, 11, 11),
}

movie_streamer = JsonStreamerPandas(movie_path)


def get_movie_data(streamer):
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    rate_per_second = 0.5  # Messages per second
    interval = 1 / rate_per_second  # Interval in seconds between messages

    curr_time = time.time()
    while True:
        if time.time() - curr_time > 6:  # Stream for 5 seconds
            break
        try:
            new_data = streamer.get_next_ten_rows()
            movies = []
            for i in range(len(new_data['item_id'])):
                movie = {key: new_data[key][i] for key in new_data}
                movies.append(movie)
            producer.send('movies', json.dumps(movies).encode('utf-8'))
            time.sleep(interval)  # Control the message rate
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue 


with DAG('movie_streaming',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    # Task for movies
    movie_task = PythonOperator(
        task_id='stream_movies',
        python_callable=get_movie_data,
        op_args=[movie_streamer],
    )

    # Define dependencies
    movie_task
    


