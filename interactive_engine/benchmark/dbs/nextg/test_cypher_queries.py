# test_cypher_queries.py

import pytest
from neo4j import GraphDatabase

@pytest.fixture(scope="module")
def neo4j_connection():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"
    driver = GraphDatabase.driver(uri, auth=(user, password))
    yield driver
    driver.close()

def execute_cypher_query(driver, query):
    with driver.session() as session:
        result = session.run(query)
        return [record.values() for record in result]

def test_match_users(neo4j_connection):
    query = "MATCH (n:User) RETURN count(n) AS user_count"
    expected_output = [[7]]
    result = execute_cypher_query(neo4j_connection, query)
    assert result == expected_output, f"Expected {expected_output}, but got {result}"

def test_cypher_queries(neo4j_connection, dataset, case, log, query, expected_output, check_order):
    result = execute_cypher_query(neo4j_connection, query)
    result_str = [list(map(str, record)) for record in result]
    print(f"Result: {result_str}")

    if check_order:
        assert result_str == expected_output, f"Test dataset '{dataset}', case '{case}', log '{log}' failed: Expected {expected_output}, but got {result_str}"
    else:
        assert set(tuple(r) for r in result_str) == set(tuple(r) for r in expected_output), f"Test dataset '{dataset}', case '{case}', log '{log}' failed: Expected {set(tuple(r) for r in expected_output)}, but got {set(tuple(r) for r in result_str)}"
