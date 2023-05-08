import pytest

from src.config import SERVER, PORT, USER, DB_NAME, PSSWD, SSL_CA
from src.db_config import sql_db_config


def test_connect_to_db():
    try:
        test_conn = sql_db_config.connect_to_db(server=SERVER, user=USER, port=PORT, db_name=DB_NAME, psswd=PSSWD, ssl_ca_path=SSL_CA)
    except Exception as e:
        pytest.fail(f"Could not connect to the database: {str(e)}")

    assert test_conn is not None
    return test_conn
