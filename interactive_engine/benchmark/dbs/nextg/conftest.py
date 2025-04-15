# conftest.py

import os
import pytest

def collect_test_files(root_dir):
    test_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.test'):
                test_files.append(os.path.join(dirpath, filename))
    return test_files

def collect_tests_from_files(test_files, dataset, cases):
    all_tests = []
    for file in test_files:
        tests = parse_test_file(file, dataset, cases)
        all_tests.extend(tests)
    return all_tests

# Add command line options
def pytest_addoption(parser):
    parser.addoption("--query_dir", action="store", default="test_files", help="Root directory to search for test files")
    parser.addoption("--cases", action="store", default=None, help="Comma separated case names to run")
    parser.addoption("--dataset", action="store", default=None, help="Specific dataset to run tests on")

# Generate tests dynamically
def pytest_generate_tests(metafunc):
    query_dir = metafunc.config.getoption("query_dir")
    cases_to_run = metafunc.config.getoption("cases")
    dataset_to_run = metafunc.config.getoption("dataset")
    
    all_test_files = collect_test_files(query_dir)

    cases_to_run_set = set(cases_to_run.split(",")) if cases_to_run else None
    
    all_tests = collect_tests_from_files(all_test_files, dataset_to_run, cases_to_run_set)
    print(f"tests: {all_tests}")

    if "dataset" in metafunc.fixturenames and "case" in metafunc.fixturenames and "log" in metafunc.fixturenames and "query" in metafunc.fixturenames and "expected_output" in metafunc.fixturenames and "check_order" in metafunc.fixturenames:
        metafunc.parametrize("dataset, case, log, query, expected_output, check_order", all_tests)

# Parse test file and extract queries and expected results
def parse_test_file(file_path, dataset=None, cases=None):
    DATASET = '-DATASET'
    CASE = '-CASE'
    LOG = '-LOG'
    STATEMENT = '-STATEMENT'
    CHECK_ORDER = '-CHECK_ORDER'
    RESULT_PREFIX = '----'

    with open(file_path, 'r') as f:
        lines = iter(f.readlines())

    tests = []
    current_dataset = None
    current_case = None
    current_test_name = None
    current_query = None
    expected_result = []
    check_order = False

    for line in lines:
        line = line.strip()
        if line.startswith(DATASET):
            current_dataset = line.split()[2]
            if dataset and current_dataset != dataset:
                print(f"Skip dataset: {current_dataset}")
                return []
        elif line.startswith(CASE):
            current_case = line.split()[1]
            if cases and current_case not in cases:
                print(f"Skip cases: {current_case}")
                return []
        elif line.startswith(LOG):
            current_test_name = line.split()[1]
            check_order = False  # 重置 `check_order`
        elif line.startswith(STATEMENT):
            current_query = line[len(STATEMENT):].strip()
        elif line.startswith(CHECK_ORDER):
            check_order = True
        elif line.startswith(RESULT_PREFIX):
            n = int(line.split()[1])
            if n == 0:
                expected_result = []
            else:
                # Read the next n lines for expected results
                expected_result = []
                for _ in range(n):
                    result_line = next(lines).strip()
                    expected_result.append(result_line.split('|'))

            tests.append((current_dataset, current_case, current_test_name, current_query, expected_result, check_order))

    return tests
