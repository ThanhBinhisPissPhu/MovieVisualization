from datetime import datetime, timedelta
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

rating_path = 'dags/streaming_data/streaming_ratings.jsonl'
review_path = 'dags/streaming_data/streaming_reviews.jsonl'

default_args = {
    'owner': 'thanhbinh',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2024, 11, 11),
}

rating_streamer = JsonStreamerPandas(rating_path)
review_streamer = JsonStreamerPandas(review_path)

def get_rating_data(streamer):
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    rate_per_second = 5  # Messages per second
    interval = 1 / rate_per_second  # Interval in seconds between messages

    curr_time = time.time()
    while True:
        if time.time() - curr_time > 50:
            break
        try:
            new_data = streamer.get_next_row()
            # print(new_data)
            processed_data = {
                "item_id": int(new_data['item_id']),
                "user_id": int(new_data['user_id']), 
                "rating": int(new_data['rating'])
            }
            producer.send('ratings', json.dumps(processed_data).encode('utf-8'))
            # logging.info(f"Data sent")
            time.sleep(interval)  # Control the message rate
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


def get_review_data(streamer):
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    rate_per_second = 4  # Messages per second
    interval = 1 / rate_per_second  # Interval in seconds between messages

    curr_time = time.time()
    while True:
        if time.time() - curr_time > 50:  # Stream for 5 seconds
            break
        try:
            new_data = streamer.get_next_row()
            producer.send('reviews', json.dumps(new_data).encode('utf-8'))
            time.sleep(interval)  # Control the message rate
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


with DAG('ratings_reviews_streaming',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    

    # Task for ratings
    rating_task = PythonOperator(
        task_id='stream_ratings',
        python_callable=get_rating_data,
        op_args=[rating_streamer],
    )

    # Task for reviews
    review_task = PythonOperator(
        task_id='stream_reviews',
        python_callable=get_review_data,
        op_args=[review_streamer],
    )

    # Define dependencies
    [rating_task, review_task]
    


