import os


def ingest_into_spark_df(spark_session):
    # define list to CSV files to load
    csv_file_paths = []

    # define dict to store df name and corresponding df
    stored_dfs = {}

    # getting the csv from the output directory
    for file in os.listdir('../resources'):
        csv_file_paths.append(file)

    # loop through the list of CSV files and load them into PySpark one at a time
    for file in csv_file_paths:
        # load the CSV into a DataFrame
        df = spark_session.read.csv(file, header=True, inferSchema=True)

        split_file_name = file.split('.')

        # store df and corresponding name in dict
        stored_dfs[split_file_name[0]] = df

    return stored_dfs

