import os


def ingest_into_spark_df(spark_session, csv_path):
    print("Ingesting CSVs into PySpark DataFrames...")

    # define dict to store df name and corresponding df
    stored_dfs = {}

    # loop through the list of CSV files and load them into PySpark one at a time
    for file in os.listdir(csv_path):
        # load the CSV into a DataFrame
        df = spark_session.read.csv(f'{csv_path}{file}', header=True, inferSchema=True)

        # split the file name to get the first part. Will be used during transformation
        split_file_name = file.split('.')

        # store df and corresponding name in dict
        stored_dfs[split_file_name[0]] = df

    return stored_dfs

