# utils.py

import os

def collect_cypher_files(root_dir):
    cypher_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.cypher'):
                cypher_files.append(os.path.join(dirpath, filename))
    return cypher_files

def parse_cypher_file(file_path):
    with open(file_path, 'r') as f:
        queries = f.read().strip().split(';')  
    queries = [q.strip() for q in queries if q.strip()]  
    return queries
