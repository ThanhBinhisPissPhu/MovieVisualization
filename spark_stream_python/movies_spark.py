import logging
import os
import re
import pandas as pd

logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # Logs to the terminal
    ]
)
# from cassandra.cluster import Cluster
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, udf, explode, split, trim, current_timestamp, date_format, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql import Window, functions as F
import pyspark.sql.functions as psf


import psycopg2
from psycopg2.extras import execute_values


def read_movies_from_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'movies') \
            .option('startingOffsets', 'earliest') \
            .load()
                        
        logging.info("kafka dataframe movies created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe movies could not be created because: {e}")

    return spark_df


def separate_movies_year_df(spark_df):
    def extract_year(title):
        """
        Extracts the year from a movie title.

        Parameters:
        - title (str): The title of the movie, which contains the year in parentheses.

        Returns:
        - int: The extracted year as an integer, or None if no year is found.
        """
        match = re.search(r'\((\d{4})\)', title)
        if match:
            return int(match.group(1))
        return None

    # Register the function as a UDF
    extract_year_udf = udf(extract_year, IntegerType())
    # Apply the UDF to create a new column with the year
    movies = spark_df.withColumn('year', extract_year_udf(col('title')))
        
    return movies


def separate_movies_genres_df(spark_df, postgres_conn):
    # Step 1: Create a separate DataFrame for genres with 'item_id' and 'genres' columns
    movies_genres = spark_df.select('item_id', 'genres')

    # Step 2: Split genres into separate rows
    movies_genres = movies_genres.withColumn('genre', explode(split(col('genres'), r'\|')))

    # Step 3: Drop the original 'genres' column
    movies_genres = movies_genres.select('item_id', 'genre')

    # Step 4: Fetch the genres table from PostgreSQL using psycopg2
    try:
        cursor = postgres_conn.cursor()
        query = "SELECT genre_name, genre_id FROM genres"
        cursor.execute(query)
        rows = cursor.fetchall()
        # Convert the result to a Pandas DataFrame
        genres_df = pd.DataFrame(rows, columns=['genre', 'genre_id'])
        cursor.close()
    except Exception as e:
        raise Exception(f"Error fetching genres from PostgreSQL: {e}")

    # Step 5: Convert Pandas DataFrame to Spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    genres = spark.createDataFrame(genres_df)

    # Step 5: Map genre names to genre ids in movies_genres
    movies_genres = movies_genres.join(genres, on='genre', how='inner')

    # Step 6: Drop the 'genre' column from movies_genres as we have replaced it with genre_id
    movies_genres = movies_genres.select('item_id', 'genre_id')

    # Step 7: Drop the 'genres' column from the original movies DataFrame
    movies = spark_df.drop('genres')

    return movies, movies_genres


def separate_movies_actors_df(movies, postgres_conn):
    # Step 1: Create a separate DataFrame for 'starring' column
    movies_actors = movies.select('item_id', 'starring')

    # Step 2: Split 'starring' into separate rows based on multiple delimiters
    movies_actors = movies_actors.withColumn(
        'actor', 
        explode(split(col('starring'), r'[,\t\n]+'))
    )

    # Step 3: Strip any leading/trailing whitespace from actor names
    movies_actors = movies_actors.withColumn('actor', trim(col('actor')))

    # Step 4: Fetch the genres table from PostgreSQL using psycopg2
    try:
        cursor = postgres_conn.cursor()
        query = "SELECT actor_name, actor_id FROM actors"
        cursor.execute(query)
        rows = cursor.fetchall()
        # Convert the result to a Pandas DataFrame
        actors_df = pd.DataFrame(rows, columns=['actor', 'actor_id'])
        cursor.close()
    except Exception as e:
        raise Exception(f"Error fetching actors from PostgreSQL: {e}")

    # Step 5: Convert Pandas DataFrame to Spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    actors = spark.createDataFrame(actors_df)

    # Step 6: Map actor names to actor ids in movies_actors
    movies_actors = movies_actors.join(actors, on='actor', how='inner')

    # Step 7: Drop the 'actor' column from movies_actors as we have replaced it with actor_id
    movies_actors = movies_actors.select('item_id', 'actor_id')

    # Step 8: Drop the 'starring' column from the original movies DataFrame
    movies = movies.drop('starring')

    return movies, movies_actors


def separate_movies_directors_df(movies, postgres_conn):
    # Step 1: Create a separate DataFrame for 'directedBy' column
    movies_directors = movies.select('item_id', 'directedBy')

    # Step 2: Split 'directedBy' into separate rows based on multiple delimiters
    movies_directors = movies_directors.withColumn(
        'director', 
        explode(split(col('directedBy'), r'[,\t\n]+'))
    )

    # Step 3: Strip any leading/trailing whitespace from director names
    movies_directors = movies_directors.withColumn('director', trim(col('director')))

    # Step 4: Fetch the genres table from PostgreSQL using psycopg2
    try:
        cursor = postgres_conn.cursor()
        query = "SELECT director_name, director_id FROM directors"
        cursor.execute(query)
        rows = cursor.fetchall()
        # Convert the result to a Pandas DataFrame
        directors_df = pd.DataFrame(rows, columns=['director', 'director_id'])
        cursor.close()
    except Exception as e:
        raise Exception(f"Error fetching directors from PostgreSQL: {e}")

    # Step 5: Convert Pandas DataFrame to Spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    directors = spark.createDataFrame(directors_df)

    # Step 6: Map director names to director ids in movies_directors
    movies_directors = movies_directors.join(directors, on='director', how='inner')

    # Step 7: Drop the 'director' column from movies_directors as we have replaced it with director_id
    movies_directors = movies_directors.select('item_id', 'director_id')

    # Step 8: Drop the 'directedBy' column from the original movies DataFrame
    movies = movies.drop('directedBy')

    return movies, movies_directors


def create_selection_df_movies(spark_df):
    """
    Transform Kafka streaming data to structured DataFrame
    :param spark_df: Streaming DataFrame from Kafka
    :return: Transformed DataFrame
    """
    schema = ArrayType(StructType([
        StructField("item_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("genres", StringType(), False),
        StructField("directedBy", StringType(), False),
        StructField("starring", StringType(), False),
        StructField("imdbId", StringType(), False),
        StructField("positive_reviews", StringType(), False),
        StructField("negative_reviews", StringType(), False),
        StructField("neutral_reviews", StringType(), False),
    ]))

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col('value'), schema).alias('data')) \
                .selectExpr("explode(data) as movie") \
                .select("movie.*")
    sel.printSchema()


    # def foreach_batch_function(batch_df, epoch_id):
    #     # Convert batch to a list of dictionaries
    #     batch_df.show(truncate=False)
        
    # streaming_query = sel.writeStream\
    #                     .foreachBatch(foreach_batch_function)\
    #                     .trigger(processingTime="2 seconds") \
    #                     .start()
    # streaming_query.awaitTermination()

    return sel


def insert_movies(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie ratings
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO movies (item_id, title, "imdbId", year, positive_reviews, negative_reviews, neutral_reviews, timestamp, "avgRating")
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'],
                    row['title'],
                    row['imdbId'],
                    row['year'],
                    row['positive_reviews'],
                    row['negative_reviews'],
                    row['neutral_reviews'],
                    row['timestamp'],
                    row['avgRating']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} rows movies inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert movies due to {e}")
    finally:
        cursor.close()


def insert_movies_genres(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie genres
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO movies_genres (item_id, genre_id)
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'], row['genre_id']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} rows movies_genres inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert movies_genres due to {e}")
    finally:
        cursor.close()


def insert_movies_actors(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie actors
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO movies_actors (item_id, actor_id)
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'], row['actor_id']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} rows movies_actors inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert movies_actors due to {e}")
    finally:
        cursor.close()


def insert_movies_directors(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie directors
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO movies_directors (item_id, director_id)
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'], row['director_id']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} rows movies_directors inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert movies_directors due to {e}")
    finally:
        cursor.close()