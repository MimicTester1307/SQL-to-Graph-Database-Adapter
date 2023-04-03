import pandas as pd
from connect_to_db import connect_to_db

# connect to the database
conn = connect_to_db()


def _extract_data_from_table(table_name='', batch_size=100000):
    # set the total number of rows in the table
    total_rows = conn.execute(f'SELECT COUNT(*) FROM {table_name};').fetchone()[0]

    # creating a generator to extract the table in batches
    for offset in range(0, total_rows, batch_size):
        query = f'SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset};'
        yield pd.read_sql_query(query, conn)  # coerce_float parameter to True


def write_table_data_to_csv(output_location='../resources', table_list=None, batch_size=1000000):
    if table_list is None:
        table_list = []
    for table_name in table_list:
        # concatenate the batches into a single dataframe
        df = pd.concat(_extract_data_from_table(table_name=table_name, batch_size=batch_size), ignore_index=True)

        # save the dataframe as a CSV file
        df.to_csv(f'{output_location}{table_name}.csv', index=False)


# close the connection
conn.close()

