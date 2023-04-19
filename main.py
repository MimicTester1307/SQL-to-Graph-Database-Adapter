import os
from dotenv import load_dotenv
from source.db_config.sql_db_config import connect_to_db

# connect to the database
load_dotenv()    # load environment variables

SERVER = os.environ.get("DB_SERVER")
PORT = int(os.environ.get("DB_PORT"))
USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DATABASE")
PSSWD = os.environ.get("DB_PSSWD")

conn = connect_to_db(server=SERVER, port=PORT, user=USER, psswd=PSSWD, db_name=DB_NAME)






























# close the connection
conn.close()