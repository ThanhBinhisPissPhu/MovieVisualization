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
from pyspark.sql.functions import from_json, col, udf, explode, split, trim, current_timestamp, date_format, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Window, functions as F
import pyspark.sql.functions as psf


import psycopg2
from psycopg2.extras import execute_values


def read_ratings_from_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'ratings') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe ratings created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe ratings could not be created because: {e}")

    return spark_df


def create_selection_df_ratings(spark_df):
    """
    Transform Kafka streaming data to structured DataFrame
    :param spark_df: Streaming DataFrame from Kafka
    :return: Transformed DataFrame
    """
    schema = StructType([
        StructField("item_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("rating", StringType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info("Transformed Kafka DataFrame ratings schema: ")
    sel.printSchema()
    return sel


def update_avg_rating(connection, data):
    """
    Update average rating of a movie in PostgreSQL database for specific item_ids.
    :param connection: PostgreSQL connection object
    :param data: DataFrame containing a list of item_ids to update
    """
    try:
        cursor = connection.cursor()

        # Extract unique item_ids from the list of dictionaries
        item_ids = list({record['item_id'] for record in data})

        if not item_ids:
            logging.info("No item_ids provided for updating.")
            return

        # Convert item_ids to a comma-separated string for SQL IN clause
        item_ids_str = ",".join(map(str, item_ids))

        # Fetch average ratings for the specific item_ids from the ratings table
        fetch_query = f"""
            SELECT item_id, AVG(rating) AS "avgRating"
            FROM ratings
            WHERE item_id IN ({item_ids_str})
            GROUP BY item_id
        """
        cursor.execute(fetch_query)
        avg_ratings = cursor.fetchall()  # Fetch all results

        # Prepare data for updating the movies table
        update_query = """
            UPDATE movies
            SET "avgRating" =ROUND(%s, 2)
            WHERE item_id = %s
        """
        values = [(row[1], row[0]) for row in avg_ratings]  # (avg_rating, item_id)

        # Batch update the movies table
        cursor.executemany(update_query, values)
        connection.commit()

        logging.info(f"{len(values)} average ratings updated successfully for the provided item_ids!")
    except Exception as e:
        logging.error(f"Could not update average ratings due to: {e}")
    finally:
        cursor.close()


def insert_ratings(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie ratings
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO ratings (item_id, user_id, rating, timestamp)
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'], row['user_id'], row['rating'], row['timestamp']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} rating rows inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert ratings due to {e}")
    finally:
        cursor.close()