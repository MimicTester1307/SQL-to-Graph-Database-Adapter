from pyspark.sql import DataFrame
from typing import Dict
import json
from neo4j.exceptions import ServiceUnavailable
import logging


def transform_df_to_node(neo4j_instance_object, df_dict: Dict[str, DataFrame]):
    """Transforms the given Spark dataframes into Neo4jNodes

    :param neo4j_instance_object: an object representing an AURADB instance
    :param df_dict: a dictionary representing a dataframe name (key) and the corresponding dataframe (value)
                    the key serves as the Node label
    :return: None
    """
    print("Beginning Transformation of Data Frames to Nodes...")

    # start aura session
    session = neo4j_instance_object.driver.session()
    for df_name, df in df_dict.items():
        # convert PySpark DataFrame to a list of dictionaries
        data = df.toJSON().map(lambda j: json.loads(j)).collect()

        # create a query string to create the nodes in Neo4j
        node_name = df_name
        columns = df.columns
        query = f"UNWIND $data AS row CREATE (: `{node_name.title()}` {{ {', '.join([f'{column}: row.{column}' for column in columns])} }})"

        # execute query using Neo4j driver session
        try:
            session.run(query, data=data)  # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")  # TODO: Fix log (redirect output to file)
            raise

    session.close()
