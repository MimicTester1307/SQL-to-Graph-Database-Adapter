from pyspark.sql import DataFrame
from typing import Dict

from neo4j.exceptions import ServiceUnavailable

import logging
import json


def transform_df_to_node(neo4j_instance_object, df_dict: Dict[str, DataFrame]):
    """Transforms the given Spark dataframes into Neo4jNodes and creates relationships between them

    :param neo4j_instance_object: an object representing an AURADB instance
    :param df_dict: a dictionary representing a dataframe name (key) and the corresponding dataframe (value)
                    the key serves as the Node label
    :return: None
    """
    print("Beginning Transformation of Data Frames to Nodes...")
    for df_name, df in df_dict.items():
        # Get list of column names from Pypark DataFrame
        columns = df.columns

        # Loop through rows in PySpark DataFrame
        for row in df.collect():
            # Create Neo4j node with label and properties
            node = {"label": df_name.capitalize()}
            for key, value in row.asDict().items():
                node[key] = value

            # Merge node into Neo4j db to prevent duplicates
            with neo4j_instance_object.driver.session() as session:
                session.write_transaction(create_node, node, columns)


# Function to create node in Neo4j database
def create_node(tx, node, columns):
    query = "MERGE (n: {label} {{{props}}}) RETURN n"
    props = ", ".join(["{}: ${}".format(column, column) for column in columns])
    result = tx.run(query.format(label=node["label"], props=props), **node)
    return result.single()[0]