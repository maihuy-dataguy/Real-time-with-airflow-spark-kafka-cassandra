import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
                INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                    post_code, email, username, dob, registered_date, phone, picture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address,
                  postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info("Spark connection created successfully")
    except Exception as e:
        logging.error(f"Couldn't create spark connection due to {e}")

    return s_conn


def create_cassandra_connection():
    session = None
    try:
        # Connect to cassandra cluster
        cluster = Cluster(['localhost'])

        session = cluster.connect()
    except Exception as e:
        logging.error(f"Couldn't create cassandra connection due to {e}")

    return session


def connect_to_kafka(spark: SparkSession):
    spark_df = None
    try:
        spark_df = spark.readStream \
            .format("kafka") \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        print("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f'Dataframe streaming could not be created due to {e}')

    return spark_df


def cassandra_sink(df: DataFrame):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sink = df.selectExpr("CAST(value as STRING)") \
        .select(from_json(col('value'), schema).alias("data")).select("data.*")

    return sink


if __name__ == '__main__':
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        df = connect_to_kafka(spark_conn)
        df_sink = cassandra_sink(df)
        cassandra_session = create_cassandra_connection()

        if cassandra_session is not None:
            create_keyspace(cassandra_session)
            create_table(cassandra_session)
            # insert_data(cassandra_session)
            logging.info("Streaming is being started...")

            streaming_query = (df_sink.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()

