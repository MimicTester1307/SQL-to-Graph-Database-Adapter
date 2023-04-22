# from neo4j import Neo4jDataFrameWriter
from pyspark.sql import DataFrame
from typing import Dict


def transform_df_to_node(df_dict: Dict[str, DataFrame], config_dict: dict):
    """Transforms the supplied Spark dataframes into Neo4j nodes by writing the dataframe directly into
    an AuraDB instance

    :param df_dict: dictionary representing a dataframe name (key) and the corresponding dataframe (value)
                    the key serves as the Node label
    :param config_dict: a dictionary representing the Neo4j instance configuration details
    :return: None
    """

    print("Beginning Transformation of Data Frames to Nodes...")
    # start aura session

    uri = config_dict.get("uri")
    user = config_dict.get("user")
    password = config_dict.get("password")
    for df_name, df in df_dict.items():
        node_param = {df_name[0] + df_name[-1]}
        try:
            (df.write
             .format("org.neo4j.spark.DataSource")
             .mode("append")
             .option("url", uri)
             .option("authentication.type", "basic")
             .option("authentication.basic.username", user)
             .option("authentication.basic.password", password)
             .option("labels", f"{node_param}:{df_name.capitalize()}")
             .save()
             )
        except Exception as e:
            print(f"error when writing dataframe to graph due to {e}")  # TODO: change to logging
