import os
from dotenv import load_dotenv

# connect to the database
load_dotenv()    # load environment variables

# SQL Database Configurations
SERVER = os.environ.get("DB_SERVER")
PORT = int(os.environ.get("DB_PORT"))
USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DATABASE")
PSSWD = os.environ.get("DB_PSSWD")
SSL_CA = os.environ.get("SSL_CA")

# Neo4j Database Configurations
BOLT_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USRNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWD")


CSV_FOLDER_PATH= "src/out/"  # CSV folder path