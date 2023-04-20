import pandas as pd
import typing


def _extract_data_from_table(conn, table_name='', batch_size=10000) -> typing.Any:
    """Extracts data from the table in batches using specified cql connection

    :param conn: Mysql connection to use; supports sqlalchemy object and pymysql object
    :param table_name: table to extract data from
    :param batch_size: batch size to use. default is 10000
    :return: generator representing table data in specified batches
    """
    with conn.cursor() as cursor:
        # get the total number of rows in the table
        cursor.execute(f'''SELECT COUNT(*) FROM {table_name};''')
        total_rows = cursor.fetchone()[0]

    # creating a generator to extract the table in batches
    print("Extracting Tables...")    # change to log
    for offset in range(0, total_rows, batch_size):
        query = f'''SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset};'''
        yield pd.read_sql_query(query, conn, coerce_float=True)  # coerce_float parameter to True


def write_table_data_to_csv(output_location='src/out/', table_list=None, batch_size=1000000, conn=None):
    """Writes extracted table data to CSV in specified output location

    :param output_location: folder where CSVs will be stored
    :param table_list: list containing database tables to extract data from
    :param batch_size: batch size in which to extract table data
    :param conn: Mysql connection to use; supports sqlalchemy object and pymysql object
    :return: None
    """
    if table_list is None:
        table_list = []
    for table_name in table_list:
        # concatenate the batches into a single dataframe
        df = pd.concat(_extract_data_from_table(table_name=table_name, batch_size=batch_size, conn=conn), ignore_index=True)

        # save the dataframe as a CSV file
        print("Exporting to CSV...")
        df.to_csv(f'{output_location}{table_name}.csv', index=False)


