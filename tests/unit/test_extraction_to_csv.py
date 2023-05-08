import pytest
import os
import pandas as pd

from test_sql_connection import test_connect_to_db
from src.scripts import extract_table_to_csv


def test_extract_table_to_csv():
    output_location = 'src/out/'
    table_list = table_names = ['products', 'orders', 'order_details']
    batch_size = 1000000
    test_conn = test_connect_to_db()

    assert table_list is not None

    for table_name in table_list:
        try:
            df = pd.concat(extract_table_to_csv._extract_data_from_table(table_name=table_name, batch_size=batch_size,
                                                                         conn=test_conn), ignore_index=True)
        except Exception as e:
            pytest.fail("Could not extract data from specified database tables")

        try:
            df.to_csv(f'{output_location}{table_name}.csv', index=False)
        except Exception as e:
            pytest.fail("Could not convert extracted data to CSV")

    for table_name in table_names:
        assert os.path.isfile(f'{output_location}{table_name}.csv')
