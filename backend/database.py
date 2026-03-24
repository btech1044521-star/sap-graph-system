from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class Neo4jConnection:
    _driver = None

    @classmethod
    def get_driver(cls):
        if cls._driver is None:
            cls._driver = GraphDatabase.driver(
                NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
        return cls._driver

    @classmethod
    def close(cls):
        if cls._driver:
            cls._driver.close()
            cls._driver = None


def get_session():
    driver = Neo4jConnection.get_driver()
    return driver.session()


def run_cypher(query: str, params: dict = None, timeout: int = None):
    with get_session() as session:
        if timeout:
            result = session.run(query, params or {}, timeout=timeout)
        else:
            result = session.run(query, params or {})
        return [record.data() for record in result]
