import pytest
from neo4j import GraphDatabase
from utils import collect_cypher_files, parse_cypher_file

class CypherBenchmark:
    def __init__(self, cypher_dir, neo4j_uri, neo4j_user, neo4j_password):
        self.cypher_dir = cypher_dir
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def __enter__(self):
        self.session = self.driver.session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        self.driver.close()

    def execute_cypher_query(self, query):
        result = list(self.session.run(query))
        return [record.values() for record in result]

@pytest.fixture(scope="module")
def neo4j_connection(request):
    cypher_dir = request.config.getoption("cypher_dir")
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    with CypherBenchmark(cypher_dir=cypher_dir, neo4j_uri=neo4j_uri, neo4j_user=neo4j_user, neo4j_password=neo4j_password) as benchmark:
        yield benchmark

class TestCypherBenchmark:
    def test_benchmark_cypher_queries(self, request, benchmark, neo4j_connection, filename, query):
        iterations = request.config.getoption("iterations")
        rounds = request.config.getoption("rounds")
        warmup_rounds = request.config.getoption("warmup_rounds")

        result = benchmark.pedantic(
            neo4j_connection.execute_cypher_query, 
            args=(query,), 
            iterations=iterations, 
            rounds=rounds, 
            warmup_rounds=warmup_rounds
        )
        print(f"File: {filename}, Query: {query}, Result: {result}")

if __name__ == '__main__':
    pytest.main()
