import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# A keyspace is like a schema

def create_keyspace(session):
    pass

def create_table(session):
    pass

def insert_data(session, **kwargs):
    pass

def create_spark_connection():
    s_conn = None
    
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1",
                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully :))")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")

    return s_conn
            
def create_cassandra_connection():
    pass

if __name__ == "__main__":
    spark_conn = create_spark_connection()