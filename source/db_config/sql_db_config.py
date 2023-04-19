import mysql.connector
from mysql.connector import errorcode
import os
from dotenv import load_dotenv

# loading the environment variables
load_dotenv()

SERVER = os.environ.get("DB_SERVER")
PORT = int(os.environ.get("DB_PORT"))
USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DATABASE")
PSSWD = os.environ.get("DB_PSSWD")


def connect_to_db(server: str, port: int, user: str, db_name: str, psswd: str):
    config = {
        'host': server,
        'user': user,
        'password': psswd,
        'database': db_name
    }
    # construct connection string
    try:
        conn = mysql.connector.connect(**config)
        print("Connection established")  # change to log
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("Database connection failed due to {}".format(err))  # change to logging
    else:
        cursor = conn.cursor()

    return cursor
