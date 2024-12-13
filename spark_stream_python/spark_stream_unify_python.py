import logging
import os
import re


logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Logs to the terminal
    ]
)
# from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, explode, split, trim,\
                                current_timestamp, date_format, from_utc_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


import psycopg2
from psycopg2.extras import execute_values

from ratings_spark import read_ratings_from_kafka, create_selection_df_ratings, insert_ratings, update_avg_rating
from movies_spark import read_movies_from_kafka, create_selection_df_movies,\
                            separate_movies_year_df, separate_movies_genres_df,\
                            separate_movies_actors_df, separate_movies_directors_df,\
                            insert_movies, insert_movies_genres, insert_movies_actors, insert_movies_directors
from reviews_spark import read_reviews_from_kafka, create_selection_df_reviews, insert_reviews, increase_reviews_count


def create_postgres_connection():
    """
    Establish connection to the PostgreSQL database
    :return: PostgreSQL connection object
    """
    try:
        connection = psycopg2.connect(
            dbname="movielens",
            user="admin",
            password="admin",
            host="localhost",
            port="5432"
        )
        logging.info("PostgreSQL connection established successfully!")
        return connection
    except Exception as e:
        logging.error(f"Could not establish PostgreSQL connection due to {e}")
        return None


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
                    .appName('SparkDataStreaming') \
                    .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
                    .getOrCreate()
        #,org.postgresql:postgresql:42.5.4") \
        current_dir = os.getcwd()
        logging.info(f"CURRENT WORKING DIR: {current_dir}")

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def process_stream(spark_df, selection_function, processing_function, postgres_conn):
    """
    Process a Kafka stream for a specific topic.
    
    :param spark_df: Input Spark DataFrame from Kafka
    :param selection_function: Function to create a selection DataFrame
    :param processing_function: Function to process each batch of the stream
    :param postgres_conn: PostgreSQL connection object
    """
    if spark_df:
        selection_df = selection_function(spark_df)
        if selection_df:
            selection_df_with_timestamp = selection_df \
                .withColumn(
                    "timestamp",
                    date_format(
                        from_utc_timestamp(current_timestamp(), "GMT+7"),
                        "yyyy-MM-dd HH:mm:ss"
                    )
                )
            logging.info("Streaming is being started...")

            # Process streaming data
            streaming_query = selection_df_with_timestamp.writeStream \
                .foreachBatch(lambda batch_df, epoch_id: processing_function(batch_df, postgres_conn)) \
                .trigger(processingTime="2 seconds") \
                .start()

            streaming_query.awaitTermination()


def process_ratings(batch_df, postgres_conn):
    # batch_df.show(truncate=False)
    data = [row.asDict() for row in batch_df.collect()]
    insert_ratings(postgres_conn, data)
    update_avg_rating(postgres_conn, data)
    

def process_movies(batch_df, postgres_conn):
    movies, movies_genres = separate_movies_genres_df(batch_df, postgres_conn)
    movies, movies_actors = separate_movies_actors_df(movies, postgres_conn)
    movies, movies_directors = separate_movies_directors_df(movies, postgres_conn)

    movies_data = [row.asDict() for row in movies.collect()]
    movies_genres_data = [row.asDict() for row in movies_genres.collect()]
    movies_actors_data = [row.asDict() for row in movies_actors.collect()]
    movies_directors_data = [row.asDict() for row in movies_directors.collect()]

    insert_movies(postgres_conn, movies_data)
    insert_movies_genres(postgres_conn, movies_genres_data)
    insert_movies_actors(postgres_conn, movies_actors_data)
    insert_movies_directors(postgres_conn, movies_directors_data)

def process_reviews(batch_df, postgres_conn):
    data = [row.asDict() for row in batch_df.collect()]
    insert_reviews(postgres_conn, data)
    increase_reviews_count(postgres_conn, data)




if __name__ == "__main__":
    # Create Spark and PostgreSQL connections
    spark_conn = create_spark_connection()
    postgres_conn = create_postgres_connection()

    if spark_conn and postgres_conn:
        # Read streams from Kafka
        movies_df = read_movies_from_kafka(spark_conn)
        ratings_df = read_ratings_from_kafka(spark_conn)
        reviews_df = read_reviews_from_kafka(spark_conn)

        # List to store all streaming queries
        queries = []

        # Process Movies Stream
        if movies_df:
            selection_movies_df = create_selection_df_movies(movies_df)
            if selection_movies_df:
                selection_movies_df = separate_movies_year_df(selection_movies_df)
                selection_movies_df_with_timestamp = selection_movies_df \
                    .withColumn(
                        "timestamp",
                        date_format(
                            from_utc_timestamp(current_timestamp(), "GMT+7"),
                            "yyyy-MM-dd HH:mm:ss"
                        )
                    )\
                    .withColumn("avgRating", lit(0))
                
                logging.info("Starting movies stream...")
                movies_query = selection_movies_df_with_timestamp.writeStream \
                    .foreachBatch(lambda batch_df, epoch_id: process_movies(batch_df, postgres_conn)) \
                    .trigger(processingTime="10 seconds") \
                    .start()
                queries.append(movies_query)


        # Process Ratings Stream
        if ratings_df:
            selection_ratings_df = create_selection_df_ratings(ratings_df)
            if selection_ratings_df:
                selection_ratings_df_with_timestamp = selection_ratings_df \
                    .withColumn(
                        "timestamp",
                        date_format(
                            from_utc_timestamp(current_timestamp(), "GMT+7"),
                            "yyyy-MM-dd HH:mm:ss"
                        )
                    )
                logging.info("Starting ratings stream...")
                ratings_query = selection_ratings_df_with_timestamp.writeStream \
                    .foreachBatch(lambda batch_df, epoch_id: process_ratings(batch_df, postgres_conn)) \
                    .trigger(processingTime="5 seconds") \
                    .start()
                queries.append(ratings_query)


        # Process Reviews Stream
        if reviews_df:
            selection_reviews_df = create_selection_df_reviews(reviews_df)
            if selection_reviews_df:
                selection_reviews_df_with_timestamp = selection_reviews_df \
                    .withColumn(
                        "timestamp",
                        date_format(
                            from_utc_timestamp(current_timestamp(), "GMT+7"),
                            "yyyy-MM-dd HH:mm:ss"
                        )
                    )
                logging.info("Starting reviews stream...")
                reviews_query = selection_reviews_df_with_timestamp.writeStream \
                    .foreachBatch(lambda batch_df, epoch_id: process_reviews(batch_df, postgres_conn)) \
                    .trigger(processingTime="5 seconds") \
                    .start()
                queries.append(reviews_query)

        # Monitor all queries
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logging.info("Stopping all streaming queries...")
            for query in queries:
                query.stop()

        postgres_conn.close()