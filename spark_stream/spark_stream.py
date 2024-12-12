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
                                current_timestamp, date_format, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
import pyspark.sql.functions as psf


import psycopg2
from psycopg2.extras import execute_values

from ratings_spark import read_ratings_from_kafka, create_selection_df_ratings, insert_ratings
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
            host="pgdatabase",
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


if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()
    postgres_conn = create_postgres_connection()

    if spark_conn and postgres_conn:
        #-------------------------------  Ratings  --------------------------------
        # Connect to Kafka with Spark connection
        # ratings_df = read_ratings_from_kafka(spark_conn)
        # if ratings_df:
        #     selection_ratings_df = create_selection_df_ratings(ratings_df)

        #     if selection_ratings_df:
        #         selection_ratings_df_with_timestamp = selection_df \
        #             .withColumn(
        #                 "timestamp",
        #                 date_format(
        #                     from_utc_timestamp(current_timestamp(), "GMT+7"),
        #                     "yyyy-MM-dd HH:mm:ss"
        #                 )
        #             )
        #         logging.info("Streaming is being started...")

        #         # Process streaming data
        #         def foreach_batch_function(batch_df, epoch_id):
        #             # Convert batch to a list of dictionaries
        #             batch_df.show(truncate=False)
        #             data = [row.asDict() for row in batch_df.collect()]
        #             insert_ratings(postgres_conn, data)

        #         streaming_ratings_query = selection_ratings_df_with_timestamp.writeStream\
        #                             .foreachBatch(foreach_batch_function)\
        #                             .trigger(processingTime="2 seconds") \
        #                             .start()
        #         streaming_ratings_query.awaitTermination()

        # postgres_conn.close()

        #-------------------------------  Movies  --------------------------------
        movies_df = read_movies_from_kafka(spark_conn)
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
                    )

                logging.info("Streaming is being started...")

                # Process streaming data
                def foreach_movies_batch_function(batch_df, epoch_id):
                    # Convert batch to a list of dictionaries
                    movies, movies_genres = separate_movies_genres_df(batch_df, postgres_conn)
                    movies, movies_actors = separate_movies_actors_df(movies, postgres_conn)
                    movies, movies_directors = separate_movies_directors_df(movies, postgres_conn)

                    # movies.show(truncate=False)
                    # movies_genres.show(truncate=False)
                    # movies_actors.show(truncate=False)
                    # movies_directors.show(truncate=False)

                    movies_data = [row.asDict() for row in movies.collect()]
                    movies_genres_data = [row.asDict() for row in movies_genres.collect()]
                    movies_actors_data = [row.asDict() for row in movies_actors.collect()]
                    movies_directors_data = [row.asDict() for row in movies_directors.collect()]
                    
                    insert_movies(postgres_conn, movies_data)
                    insert_movies_genres(postgres_conn, movies_genres_data)
                    insert_movies_actors(postgres_conn, movies_actors_data)
                    insert_movies_directors(postgres_conn, movies_directors_data)


                streaming_movies_query = selection_movies_df_with_timestamp.writeStream\
                                .foreachBatch(foreach_movies_batch_function)\
                                .trigger(processingTime="2 seconds") \
                                .start()

                streaming_movies_query.awaitTermination()
            
        postgres_conn.close()

        #-------------------------------  Reviews  --------------------------------

        # reviews_df = read_reviews_from_kafka(spark_conn)
        # if reviews_df:
        #     selection_reviews_df = create_selection_df_reviews(reviews_df)

        #     if selection_reviews_df:
        #         selection_reviews_df_with_timestamp = selection_reviews_df \
        #             .withColumn(
        #                 "timestamp",
        #                 date_format(
        #                     from_utc_timestamp(current_timestamp(), "GMT+7"),
        #                     "yyyy-MM-dd HH:mm:ss"
        #                 )
        #             )
        #         logging.info("Streaming is being started...")

        #         # Process streaming data
        #         def foreach_batch_function(batch_df, epoch_id):
        #             # Convert batch to a list of dictionaries
        #             # batch_df.show(truncate=False)
        #             data = [row.asDict() for row in batch_df.collect()]
        #             insert_reviews(postgres_conn, data)
        #             increase_reviews_count(postgres_conn, data)

        #         streaming_reviews_query = selection_reviews_df_with_timestamp.writeStream\
        #                             .foreachBatch(foreach_batch_function)\
        #                             .trigger(processingTime="2 seconds") \
        #                             .start()
        #         streaming_reviews_query.awaitTermination()

        # postgres_conn.close()
