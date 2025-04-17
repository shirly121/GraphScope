# conftest.py

import pytest
import os
from utils import collect_cypher_files, parse_cypher_file

def pytest_addoption(parser):
    parser.addoption("--cypher_dir", action="store", default="cypher_queries", help="Directory containing .cypher files")
    parser.addoption("--iterations", action="store", type=int, default=5, help="Number of iterations for each benchmark")
    parser.addoption("--rounds", action="store", type=int, default=10, help="Number of rounds for each benchmark")
    parser.addoption("--warmup_rounds", action="store", type=int, default=1, help="Number of warmup rounds for each benchmark")

def pytest_generate_tests(metafunc):
    cypher_dir = metafunc.config.getoption("cypher_dir")

    all_cypher_files = collect_cypher_files(cypher_dir)
    print(f"all cypher files: {all_cypher_files}")

    all_queries = []
    for file in all_cypher_files:
        queries = parse_cypher_file(file)
        filename = os.path.basename(file)
        for query in queries:
            all_queries.append((filename, query))
    
    print(f"all cypher queries: {all_queries}")

    if "filename" in metafunc.fixturenames and "query" in metafunc.fixturenames:
        metafunc.parametrize("filename, query", all_queries)
