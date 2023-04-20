from neo4j import GraphDatabase


class App:
    """
    A wrapper around the Neo4J GraphDatabase driver.
    """
    def __init__(self, uri, user, password):
        """Initializes the driver for connection to AuraDB instance
        :param uri: bolt connection URI for neo4j AuraDB instance
        :param user: AuraDB instance username
        :param password: AuraDB instance user password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
        closes the driver connection to AuraDB instance
        :return: None
        """
        self.driver.close()
