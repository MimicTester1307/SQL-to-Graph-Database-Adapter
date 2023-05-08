import os

import pytest
from pyspark.sql import SparkSession

from src.scripts import ingest_csv_to_pyspark


def test_ingest_into_spark_df():
    try:
        test_spark = SparkSession.builder.appName("Ingest Multiple CSV to PySpark")\
            .config('spark.jars.packages', 'neo4j-contrib:neo4j-connector-apache-spark_2.12:4.0.1_for_spark_3')\
            .getOrCreate()

        csv_path = "src/out/"

        test_dict_df = ingest_csv_to_pyspark.ingest_into_spark_df(spark_session=test_spark, csv_path=csv_path)
    except Exception as e:
        pytest.fail("Unable tp ingest CSV files")

    # count number of files in specified directory. number of dataframes in dict must match number of files in location
    num_files = len([name for name in os.listdir(csv_path) if os.path.isfile(os.path.join(csv_path, name)) and name.endswith('.csv')])
    assert test_dict_df is not None and len(test_dict_df) == num_files
