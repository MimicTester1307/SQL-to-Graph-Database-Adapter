import pymysql
import logging

# configure logging
logging.basicConfig(
    filename='logs/sql_db_config.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.NOTSET
)



def connect_to_db(server: str, port: int, user: str, db_name: str, psswd: str, ssl_ca_path: str):
    """uses pymysql to establish connection to an Azure MySQL server

    :param server: server host/endpoint
    :param port: port to connect in
    :param user: database user to connect with
    :param db_name: database name to connect to
    :param psswd: user password
    :param ssl_ca_path: path to SSL certificate
    :return: pymysql connection object
    """
    config = {
        'host': server,
        'user': user,
        'password': psswd,
        'database': db_name,
        'port': port,
        'ssl_ca': ssl_ca_path
    }
    try:
        # create connection object
        conn = pymysql.connect(**config)
        print("Connection established")
        logging.info("Connection to database successfully established.")
    except pymysql.err.OperationalError as err:
        print("Something is wrong with the database username or password or database does not exist. See log file for "
              "more details.")
        logging.error(f"Database connection failed with error {err}.")
        raise
    except pymysql.Error as err:
        print("Database connection failed. See log file for more details.")
        logging.error(f"Database connection failed due to {err}")
        raise
    else:
        return conn
