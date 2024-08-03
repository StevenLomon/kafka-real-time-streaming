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
    pass

def create_cassandra_connection():
    pass