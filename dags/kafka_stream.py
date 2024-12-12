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

rating_path = 'dags/streaming_data/streaming_ratings.jsonl'
review_path = 'dags/streaming_data/streaming_reviews.jsonl'
movie_path = 'dags/streaming_data/streaming_movies.jsonl'

default_args = {
    'owner': 'thanhbinh',
    'start_date': datetime(2024, 11, 11),
}

rating_streamer = JsonStreamerPandas(rating_path)
review_streamer = JsonStreamerPandas(review_path)
movie_streamer = JsonStreamerPandas(movie_path)

def get_rating_data(streamer):
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    rate_per_second = 2  # Messages per second
    interval = 1 / rate_per_second  # Interval in seconds between messages

    curr_time = time.time()
    while True:
        if time.time() - curr_time > 30:
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
    rate_per_second = 0.4  # Messages per second
    interval = 1 / rate_per_second  # Interval in seconds between messages

    curr_time = time.time()
    while True:
        if time.time() - curr_time > 20:  # Stream for 5 seconds
            break
        try:
            new_data = streamer.get_next_row()
            producer.send('reviews', json.dumps(new_data).encode('utf-8'))
            time.sleep(interval)  # Control the message rate
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


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

def delay():
    time.sleep(5)

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    # Task for movies
    movie_task = PythonOperator(
        task_id='stream_movies',
        python_callable=get_movie_data,
        op_args=[movie_streamer],
    )

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

    delay_task = PythonOperator(
        task_id='delay_after_movies',
        python_callable=delay,
    )

    # Define dependencies
    movie_task >> delay_task >> [rating_task, review_task]

