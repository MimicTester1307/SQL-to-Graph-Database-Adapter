import pymysql
import boto3
import boto3.session
import os
from dotenv import load_dotenv

# loading the environment variables
load_dotenv()
DB_ENDPOINT = os.environ.get("DB_ENDPOINT")
PORT = int(os.environ.get("DB_PORT"))
USER = os.environ.get("DB_USER")
REGION = os.environ.get("DB_REGION")
DB_NAME = os.environ.get("DATABASE")
ACCESS_KEY = os.environ.get("IAM_ACCESS_KEY")
SECRET_ACCESS_KEY = os.environ.get("IAM_SECRET_ACCESS_KEY")
CERT_LOCATION = os.environ.get("CERT_LOCATION")
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


# print(DB_ENDPOINT, PORT, USER, REGION, DB_NAME)

def connect_to_db():
    session = boto3.session.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY)
    rds_client = session.client(service_name='rds', region_name=REGION)

    token = rds_client.generate_db_auth_token(DBHostname=DB_ENDPOINT, Port=PORT, DBUsername=USER)

    try:
        conn = pymysql.connect(
            host=DB_ENDPOINT,
            user=USER,
            password=token,
            port=PORT,
            database=DB_NAME,
            ssl_ca=CERT_LOCATION
        )
        # if conn.ping(reconnect=True):
        #     return conn
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        print(query_results)  # change to logging

        if conn.ping(reconnect=True):
            return conn
    except Exception as e:
        print("Database connection failed due to {}".format(e))  # change to logging


connect_to_db()
