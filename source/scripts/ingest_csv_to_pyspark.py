from pyspark.sql import SparkSession

# define list to CSV files to load
csv_file_paths = []

# create a SparkSession object
spark = SparkSession.builder.appName("CSV to PySpark").getOrCreate()

# loop through the list of CSV files and load them into PySpark one at a time
for file in csv_file_paths:
    # load the CSV into a DataFrame
    df = spark.read.csv(file, header=True, inferSchema=True)

    # print the schema and show first few rows
    df.printSchema()
    df.show(5)

spark.stop()
