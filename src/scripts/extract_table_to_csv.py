import pandas as pd
import typing
import pymysql
import logging


# configure logging
logging.basicConfig(
    filename='logs/dbtable_to_csv.log',
    filemode='a',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    level=logging.NOTSET
)


def _extract_data_from_table(conn, table_name='', batch_size=10000) -> typing.Any:
    """Extracts data from the table in batches using specified cql connection

    :param conn: Mysql connection to use; supports sqlalchemy object and pymysql object
    :param table_name: table to extract data from
    :param batch_size: batch size to use. default is 10000
    :return: generator representing table data in specified batches
    """
    try:
        with conn.cursor() as cursor:
            # get the total number of rows in the table
            cursor.execute(f'''SELECT COUNT(*) FROM {table_name};''')
            total_rows = cursor.fetchone()[0]
    except pymysql.ProgrammingError as err:
        print("There was an issue executing the query. It is likely the given table does not exist.")
        logging.warning(f"A ProgrammingError occurred with error {err}")
    except AttributeError as err:
        print("An attribute error occurred. It is most likely that fetchone was called on `None`")
        logging.error(f"An AttributeError occurred with error {err}")
    except pymysql.Error as err:
        print("An error occurred while attempting to execute a query.")
        logging.error(f"Error {err} while executing query")

    # creating a generator to extract the table in batches
    print("Extracting Tables...")    # change to log
    for offset in range(0, total_rows, batch_size):
        query = f'''SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset};'''
        yield pd.read_sql_query(query, conn, coerce_float=True)  # coerce_float parameter to True

    logging.info("Querying table data executed successfully.")



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
        try:
            df = pd.concat(_extract_data_from_table(table_name=table_name, batch_size=batch_size, conn=conn), ignore_index=True)
        except pd.errors as err:
            print("Pandas dataframe concatenation failed for some reason. See log file for more information.")
            logging.error(f"Pandas failed to concatenate table data with error: {err}")
            raise

        try:
            # save the dataframe as a CSV file
            print("Exporting to CSV...")
            df.to_csv(f'{output_location}{table_name}.csv', index=False)
        except PermissionError as err:
            print("You do not have access to write to the specified file path.")
            logging.error(f"A filepath PermissionError occurred at {err}")
            raise
        except ValueError as err:
            print("Specified file format is not valid or supported by pandas.")
            logging.error(f"File format not supported with error {err}")
            raise

    logging.info("Extracting table data into CSV completed successfully.")
