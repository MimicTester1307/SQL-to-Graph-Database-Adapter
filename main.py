import os
import logging

import pyspark.errors.exceptions.connect
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.db_config.sql_db_config import connect_to_db
from src.scripts.extract_table_to_csv import write_table_data_to_csv
from src.scripts.ingest_csv_to_pyspark import ingest_into_spark_df
from src.transformations.transform_df_to_node_spark import transform_df_to_node


# configure logging
logging.basicConfig(
    filename='logs/main.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.NOTSET
)


# connect to the database
load_dotenv()    # load environment variables

# SQL Database Configurations
SERVER = os.environ.get("DB_SERVER")
PORT = int(os.environ.get("DB_PORT"))
USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DATABASE")
PSSWD = os.environ.get("DB_PSSWD")
SSL_CA = os.environ.get("SSL_CA")


try:
    conn = connect_to_db(server=SERVER, port=PORT, user=USER, psswd=PSSWD, db_name=DB_NAME, ssl_ca_path=SSL_CA)
except NameError as err:
    print("conn is not defined. the database most likely failed to connect")
    logging.error("name conn does not exist")

# Declaring table names that will be used by system
table_names = ['products', 'orders', 'order_details']

# begin extraction process
write_table_data_to_csv(output_location='src/out/', table_list=table_names, batch_size=500, conn=conn)

# Begins CSV to PySpark Ingestion process
try:
    spark = SparkSession.builder.appName("Ingest Multiple CSV to PySpark")\
        .config('spark.jars.packages', 'neo4j-contrib:neo4j-connector-apache-spark_2.12:4.0.1_for_spark_3')\
        .getOrCreate()
except pyspark.errors.PySparkException as err:
    print(f"Error {err} when starting spark session. See log file")
    logging.error(f"Spark session failed to start, with error {err}")
    raise

csv_folder_path = "src/out/"  # CSV folder path

# Ingest CSV files into Spark data frames
df_dict = ingest_into_spark_df(spark_session=spark, csv_path=csv_folder_path)

# Loading Pandas DataFrames into Neo4j
# Neo4j Database Configurations
BOLT_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USRNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWD")

config = {
    'uri': BOLT_URI,
    'password': NEO4J_PASSWORD,
    'user': NEO4J_USER
}
# Begin transformation to nodes
transform_df_to_node(df_dict=df_dict, config_dict=config)


conn.close()
spark.stop()
