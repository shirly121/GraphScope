import os
import json
import time
import re
import kuzu
import argparse
from natsort import natsorted

def normalize_string(input_str):
    """Helper function to normalize strings for comparison."""
    normalized_str = input_str.replace("\\\"", "\"")
    normalized_str = normalized_str.replace("'", "\"")
    normalized_str = normalized_str.replace("\n", "").replace("\r", "").replace("\t", "").strip()
    normalized_str = re.sub(r"\s+", " ", normalized_str)
    return normalized_str

def load_queries(query_dir):
    """Load all .cypher queries from the specified directory."""
    queries = {}
    for fname in os.listdir(query_dir):
        if fname.endswith(".cypher"):
            with open(os.path.join(query_dir, fname), "r") as f:
                query_name = fname.replace(".cypher", "")
                queries[query_name] = f.read().strip()
    queries = {k: queries[k] for k in natsorted(queries.keys())}
    return queries

def compare_results(query_name, actual_result, expected_results):
    """Compare actual query results with expected results."""
    expected_result = expected_results.get(query_name, "").strip()
    if expected_result:
        expected_normalized = normalize_string(expected_result)
        actual_normalized = normalize_string(actual_result)
        if expected_normalized != actual_normalized:
            print(f"{query_name}: Query result does not match the expected result.")
            print("Expected:", expected_result)
            print("Actual  :", actual_result)
    else:
        print(f"{query_name}: No expected result found for comparison.")

def main(db_path, query_dir, k, parallelism, results_file=None):
    # Initialize database connection
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)
    conn.execute(f"CALL THREADS={parallelism};")

    # Load queries from the specified directory
    queries = load_queries(query_dir)

    # Load expected results from the specified JSON file (if provided)
    expected_results = {}
    if results_file:
        with open(results_file, "r") as f:
            expected_results = json.load(f)

    # Execute each query k times and calculate the average execution time
    for query_name, query in queries.items():
        total_duration = 0
        actual_result = ""

        for _ in range(k):
            start_time = time.time()  # Start timing
            response = conn.execute(query)
            actual_results = []
            while response.has_next():
                actual_results.append(str(response.get_next()).strip())

            # Joins actual results to a single string
            actual_result = " ".join(actual_results)
            end_time = time.time()  # End timing
            total_duration += (end_time - start_time) * 1000
        
        average_duration = total_duration / k
        
        if results_file:
            # Compare actual results with expected results
            compare_results(query_name, actual_result, expected_results)
        
        print(f"QueryName[{query_name}]: AverageExecuteTimeMS[{average_duration:.2f}] ms. Results: {actual_result}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute and optionally compare KuZu queries.")
    parser.add_argument("--db_path", required=True, help="Database path for KuZu.")
    parser.add_argument("--query_dir", required=True, help="Directory containing .cypher query files.")
    parser.add_argument("--k", type=int, default=1, help="Number of times each query should be executed for average time measurement.")
    parser.add_argument("--parallelism", type=int, default=1, help="Number of threads for executing queries.")
    parser.add_argument("--results_file", help="Optional JSON file containing expected results.", default=None)
    
    args = parser.parse_args()
    
    main(args.db_path, args.query_dir, args.k, args.parallelism, args.results_file)
