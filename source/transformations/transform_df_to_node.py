from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct
import os
from dotenv import load_dotenv

from source.scripts.ingest_csv_to_pyspark import ingest_into_spark_df

# loading the environment variables
load_dotenv()

# retrieve neo4j connection details
URI = os.environ.get("NEO4J_URI")
USERNAME = os.environ.get("NEO4J_USRNAME")
PASSWORD = os.environ.get("NEO4J_PASSWD")

# create a SparkSession object and call function to ingest CSV files
spark = SparkSession.builder.appName("CSV to PySpark").getOrCreate()
df_dict = ingest_into_spark_df(spark_session=spark)
spark.stop()

# define query to convert the PySpark df to a Neo4j node

for df_name, df in df_dict.items():
    query = """
    UNWIND $data as row
    MERGE (n:%s {id: row.id})
    SET n.name = row.name
    """ % (df_name.title())