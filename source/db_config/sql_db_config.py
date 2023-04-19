import mysql.connector
from mysql.connector import errorcode


def connect_to_db(server: str, port: int, user: str, db_name: str, psswd: str):
    config = {
        'host': server,
        'user': user,
        'password': psswd,
        'database': db_name,
        'port': port
    }
    # construct connection string
    try:
        conn = mysql.connector.connect(**config)
        print("Connection established")  # change to logging
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

    return None
