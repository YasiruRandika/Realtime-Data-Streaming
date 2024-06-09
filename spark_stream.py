import logging
import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """
    )

    logging.info("Cassandra Keyspace Created")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.users(
            id UUID Primary Key,
            first_name text,
            last_name text,
            gender text,
            postcode text,
            city text,
            state text,
            country text,
            email text,
            phone text,
            cell text,
            latitude float,
            longitude float,
            timezone text,
            utc text
        );
        """
    )

    logging.info("Cassandra Table Created")


def insert_data(session, **kwargs):
    logging.info("Inserting Data")

    id = kwargs['id']
    first_name = kwargs['first_name']
    last_name = kwargs['last_name']
    gender = kwargs['gender']
    postcode = kwargs['postcode']
    city = kwargs['city']
    state = kwargs['state']
    country = kwargs['country']
    email = kwargs['email']
    phone = kwargs['phone']
    cell = kwargs['cell']
    latitude = kwargs['latitude']
    longitude = kwargs['longitude']
    timezone = kwargs['timezone']
    utc = kwargs['utc']

    try:
        session.execute(
            """
            INSERT INTO spark_streams.users
            (id, first_name, last_name, gender, postcode, city, state, country, email, phone, cell, latitude, longitude, timezone, utc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (id, first_name, last_name, gender, postcode, city, state, country, email, phone, cell, latitude, longitude,
             timezone, utc)
        )

        logging.info("Data Inserted")
    except Exception as e:
        logging.error(f"Error while inserting data: {str(e)}")


def create_spark_connection():
    print("Spark Session Creating")
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("RealTimeDataStreaming") \
            .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")) \
            .config("spark.driver.host", "local[*]") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark Session Created")
    except Exception as e:
        logging.error(f"Error while creating Spark Session: {str(e)}")

    return s_conn


def create_cassandra_connection():
    try:
        # Connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        session = cluster.connect()

        return session
    except Exception as e:
        logging.error(f"Error while creating Cassandra Session: {str(e)}")
        return None


def connect_to_kafka(spark_conn):
    try:
        kafka_df = spark_conn.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users") \
            .option("startingOffsets", "earliest") \
            .load()

        logging.info("Connected to Kafka")

        return kafka_df
    except Exception as e:
        logging.error(f"Error while connecting to Kafka: {str(e)}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("location", StructType([
            StructField("coordinates", StructType([
                StructField("latitude", FloatType(), True),
                StructField("longitude", FloatType(), True)
            ]), True),
            StructField("timezone", StructType([
                StructField("offset", StringType(), True),
                StructField("description", StringType(), True)
            ]), True)
        ]), True)  # Added True and closing parenthesis here
    ])

    sel = spark_df.select(
        from_json(col("value").cast("string"), schema).alias("value")
    ).select("value.*")

    return sel


if __name__ == "main":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connecting to Kafka
        df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(df)

        cassandra_conn = create_cassandra_connection()

        if cassandra_conn is not None:
            create_keyspace(cassandra_conn)
            create_table(cassandra_conn)

            streaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .option("keyspace", "spark_streams") \
                .option("table", "users") \
                .start()

            streaming_query.awaitTermination()