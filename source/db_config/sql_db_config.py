import pymysql


def connect_to_db(server: str, port: int, user: str, db_name: str, psswd: str, ssl_ca_path: str):
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
    except pymysql.Error as err:
        if err == pymysql.err.OperationalError:
            print("Something is wrong with the username or password")
        elif err == pymysql.err.ProgrammingError:
            print("Database does not exist")
        else:
            print("Database connection failed due to {}".format(err))
    else:
        return conn