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


def read_reviews_from_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'reviews') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe reviews created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe reviews could not be created because: {e}")

    return spark_df


def create_selection_df_reviews(spark_df):
    """
    Transform Kafka streaming data to structured DataFrame
    :param spark_df: Streaming DataFrame from Kafka
    :return: Transformed DataFrame
    """
    schema = StructType([
        StructField("item_id", StringType(), False),
        StructField("txt", StringType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    logging.info("Transformed Kafka DataFrame reviews schema: ")
    sel.printSchema()
    return sel


def insert_reviews(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie reviews
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO reviews (item_id, txt)
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'], row['txt']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} rows inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert reviews due to {e}")
    finally:
        cursor.close()

