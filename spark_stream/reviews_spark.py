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

    def simple_sentiment_analysis(text):
        positive_words = {"good", "great", "excellent", "amazing", "positive", "happy"}
        negative_words = {"bad", "terrible", "horrible", "negative", "sad", "angry"}
        
        words = set(text.lower().split())
        has_positive = bool(words & positive_words)
        has_negative = bool(words & negative_words)
        
        if has_positive and has_negative:
            return "neutral"
        elif has_positive:
            return "positive"
        elif has_negative:
            return "negative"
        return "neutral"
    

    # Register UDF
    sentiment_udf = udf(simple_sentiment_analysis, StringType())
    sel = sel.withColumn("sentiment", sentiment_udf(col("txt")))
    return sel


def increase_reviews_count(connection, data):
    """
    Update the number of reviews for each sentiment type in the PostgreSQL movies table.
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie reviews
    """
    try:
        cursor = connection.cursor()
        
        # Create separate queries for each sentiment
        update_query = """
            UPDATE movies
            SET 
                positive_reviews = positive_reviews + CASE WHEN %s = 'positive' THEN 1 ELSE 0 END,
                negative_reviews = negative_reviews + CASE WHEN %s = 'negative' THEN 1 ELSE 0 END,
                neutral_reviews = neutral_reviews + CASE WHEN %s = 'neutral' THEN 1 ELSE 0 END
            WHERE item_id = %s
        """
        
        # Prepare data
        values = [(row['sentiment'], row['sentiment'], row['sentiment'], row['item_id']) for row in data]
        
        # Execute the query for each row
        cursor.executemany(update_query, values)
        connection.commit()
        logging.info(f"Updated review counts for {len(values)} rows successfully!")
    except Exception as e:
        logging.error(f"Could not update review counts due to {e}")
    finally:
        cursor.close()



def insert_reviews(connection, data):
    """
    Insert data into PostgreSQL database
    :param connection: PostgreSQL connection object
    :param data: List of rows containing movie reviews
    """
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO reviews (item_id, txt, sentiment, timestamp)
            VALUES %s
        """
        # Convert data into a list of tuples
        values = [(row['item_id'], row['txt'], row['sentiment'], row['timestamp']) for row in data]
        execute_values(cursor, query, values)
        connection.commit()
        logging.info(f"{len(values)} reviews rows inserted successfully!")
    except Exception as e:
        logging.error(f"Could not insert reviews due to {e}")
    finally:
        cursor.close()

