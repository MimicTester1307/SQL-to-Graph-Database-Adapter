import pymysql
import boto3
import os
from dotenv import load_dotenv

# loading the environment variables
load_dotenv()
ENDPOINT = os.environ.get("DB_ENDPOINT")
PORT = os.environ.get("DB_PORT")
USER = os.environ.get("DB_USER")
REGION = os.environ.get("DB_REGION")
DB_NAME = os.environ.get("DATABASE")
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


def connect_to_db():
    session = boto3.Session(profile_name='default')
    client = session.client('rds')

    token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

    try:
        conn = pymysql.connect(
            host=ENDPOINT,
            user=USER,
            password=token,
            port=PORT,
            database=DB_NAME,
            ssl_ca='SSLCERTIFICATE'
        )
        if conn.ping(reconnect=True):
            return conn
        # cur = conn.cursor()
        # cur.execute("""SELECT now()""")
        # query_results = cur.fetchall()
        # print(query_results)  # change to logging
    except Exception as e:
        print("Database connection failed due to {}".format(e))  # change to logging
