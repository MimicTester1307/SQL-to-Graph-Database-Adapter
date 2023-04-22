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
    # start aura session
    session = neo4j_instance_object.driver.session()
    for df_name, df in df_dict.items():

        # get the column names and data types of the data frame
        dtypes = df.dtypes
        columns = df.columns

        # convert PySpark DataFrame to a list of dictionaries
        data = df.toJSON().map(lambda j: json.loads(j)).collect()

        for col_, dtype in dtypes:
            # create a property string for the on-create set clause
            if "string" in dtype:
                property_string = f"{df_name[0]}.{col_} = row.{col_}"
            elif "timestamp" in dtype:
                property_string = f"{df_name[0]}.{col_} = datetime(replace(replace(row.{col_}, ' ', 'T'), 'Z', ''))"
            elif "int" in dtype:
                property_string = f"{df_name[0]}.{col_} = toInteger(row.{col_})"
            elif "double" in dtype:
                property_string = f"{df_name[0]}.{col_} = toFloat(row.{col_})"
            else:
                property_string = f"{df_name[0]}.{col_} = to{type(dtype)}(row.{col_})"  # log to unable to handle data type at the moment

            label = df_name.capitalize()

            # create a query string to create the nodes in Neo4j
            create_query = f"UNWIND $data AS row " \
                           f"MERGE ({df_name[0]}: `{label}` {{ {', '.join([f'{column}: row.{column}' for column in columns])} }})" \
                           f"ON CREATE SET {property_string}"

            # create_query = cypher_query % (df_name[0], label, key, key, property_string)
            # execute query using Neo4j driver session
            try:
                session.run(create_query,
                            data=data)  # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
            except ServiceUnavailable as exception:
                logging.error(
                    f"{create_query} raised an error: \n {exception}")  # TODO: Fix log (redirect output to file)
                raise

    session.close()


# Attempt to create relationships between nodes
# def create_relationships_between_nodes(neo4j_instance_object, rel: str, paired_table_df: DataFrame):
#     """Creates and returns relationships between the nodes
#
#     :param neo4j_instance_object: an object representing an AURADB instance
#     :param rel: the type of relationship to be formed between the nodes
#     :param paired_table_df: a table name representing the table (now node) that contains a relationship between
#                             other tables. It is usually a decomposed table containing just foreign keys. It will be used
#                             to create a relationship between other tables using the specified @rel.
#                             Note that the table names are just for reference the table entities will have been
#                             transformed to nodes at this point.
#     :return: None
#     """
#     pass