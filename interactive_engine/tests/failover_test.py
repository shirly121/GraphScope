#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2024 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    ./failover_test.py --instance_name <instance_name> --namespace <namespace> --kubeconfig <kubeconfig> --stage <stage>
"""

import argparse
import subprocess
import time
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from graphscope.framework.record import VertexRecordKey
import graphscope as gs


RETRY_COUNT = 3
TIMEOUT = "300s"
GREMLIN_PORT = 12312
GRPC_PORT = 55556
kubectl_cmd = None

def run_cmd(command):
    global kubectl_cmd
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
        return True, output.decode('utf-8')
    except subprocess.CalledProcessError as e:
        return False, e.output.decode('utf-8')

def get_pods_by_instance(instance_name):
    success, output = run_cmd(f"{kubectl_cmd} get pods -o jsonpath='{{.items[*].metadata.name}}'")
    if not success:
        print(f"Failed to get pods. Error:\n{output}")
        return []
    
    # Filter pods by checking if instance_name is part of their names
    pods = [name for name in output.split() if instance_name in name]
    return pods

def delete_and_watch(pod_name, condition="Ready"):
    print(f"Deleting pod {pod_name}...")
    success, output = run_cmd(f"{kubectl_cmd} delete pod {pod_name} --wait=false")
    if not success:
        print(f"Failed to delete pod {pod_name}. Skipping...\n{output}")
        return
    success, _ = run_cmd(f"{kubectl_cmd} wait --for=delete pod/{pod_name} --timeout={TIMEOUT}")
    start_time = time.time()
    success, output = run_cmd(f"{kubectl_cmd} wait --for=condition={condition} pod/{pod_name} --timeout={TIMEOUT}")
    if success:
        end_time = time.time()
        duration = end_time - start_time
        print(f"Pod {pod_name} became {condition} in {duration} seconds.")
    else:
        print(f"Pod {pod_name} did not become {condition} within the timeout period.\n{output}")

def get_node_ip(release_name):
    jsonpath = "{.status.podIP}"
    command = f"{kubectl_cmd} get pod {release_name}-graphscope-store-frontend-0 -o jsonpath='{jsonpath}'"
    success, node_ip = run_cmd(command)
    if not success:
        raise Exception(f"Failed to get node IP for {release_name}. Error: {node_ip}")
    else:
        return node_ip

def get_conn(release_name, grpc_port=GRPC_PORT, gremlin_port=GREMLIN_PORT):
    node_ip = get_node_ip(release_name)
    grpc_endpoint = f"{node_ip}:{grpc_port}"
    gremlin_endpoint = f"{node_ip}:{gremlin_port}"
    return gs.conn(grpc_endpoint, gremlin_endpoint)

def get_client(release_name, gremlin_port=GREMLIN_PORT):
    node_ip = get_node_ip(release_name)
    url = f"ws://{node_ip}:{gremlin_port}/gremlin"
    return Client(url, "g")

def submit_query(client, query_str):
    return client.submit(query_str)

def submit_async_query(client, query_str):
    return client.submit_async(query_str)


def perform_queries_with_retries(client, retry_count=RETRY_COUNT):
    for _ in range(retry_count):
        try:
            test_query = submit_query(client, "g.V().limit(1).count()")
            print("Query succeed, and the result is ", test_query.all().result())
            return 

        except GremlinServerError as e:
            print(f"Gremlin server error {e}, retrying...")

    raise Exception("Failed to perform queries after multiple retries.")

def get_statistics(client):
    result = submit_query(client, "g.V().hasLabel(\"PERSON\").count()")
    return result.all().result()[0]


def prepare_data():
    vertices = []
    # fake vertex properties
    vertex_properties = {
            "firstName": "John",
            "lastName": "Doe",
            "gender": "Male"
    }
    # fake vertex ids
    id_start = 10000000
    for i in range(100000):
        vertex_record = [VertexRecordKey("PERSON", {"id": id_start + i}), vertex_properties]
        vertices.append(vertex_record)
    return vertices

def delete_data(conn, vertices):
    graph = conn.g()
    snapshot_id = graph.delete_vertices([key[0] for key in vertices])
    return snapshot_id

def insert_data(conn, vertices):
    graph = conn.g()
    snapshot_id = graph.insert_vertices(vertices)
    return snapshot_id
                                

def main(args):
    global kubectl_cmd
    kubectl_cmd = f"kubectl --kubeconfig {args.kubeconfig} -n {args.namespace}"
    release_name = args.instance_name
    pods = get_pods_by_instance(release_name)
    if not pods:
        print(f"No pods found for instance {release_name}.")
        return
    stages_to_run = args.stage 

    if stages_to_run == "all":
        stages_to_run = ["1", "2", "3", "4", "5"]

    if "1" in stages_to_run:
        print(f"#########################################################")
        print(f"############## 1. try to delete all pods... #############")
        print(f"#########################################################")
        for pod in pods:
            delete_and_watch(pod)
            print()

    if "2" in stages_to_run:
        print(f"#########################################################")
        print(f"################# 2. try some queries... ################")
        print(f"#########################################################")
        client = get_client(release_name)
        person_count = get_statistics(client)
        print(f"Number of person vertices: {person_count}")

    if "3" in stages_to_run:
        print(f"#########################################################")
        print(f"############## 3. delete pod during query... ############")
        print(f"#########################################################")
        client = get_client(release_name)
        for pod in pods:
            # submit a async query before deleting the pod
            result_future = submit_async_query(client, "g.V().count()")
            # delete the pod
            delete_and_watch(pod)
            # check the result of the previous query, and perform queries after restarting the pod
            try:
                print("Previous query (before pod deleted) executed successfully, and the result is", result_future.result().all().result())
            except GremlinServerError as e:
                print(f"An GremlinServerError occurred: {e}, retrying...")
            except RuntimeError as e:
                if "Connection was already closed" in str(e):
                    print("Previous query (before pod deleted) failed, due to RuntimeError with message: Connection was already closed. Reconnecting client...")
                    client = get_client(release_name)
                else:
                    print(f"Previous query (before pod deleted) failed, due to an unexpected RuntimeError occurred: {e}")
                    raise
            except Exception as e:
                print(f"Previous query (before pod deleted) failed, due to an unexpected error occurred: {e}")
                raise

            print(f"Performing queries after restarting {pod}...")
            perform_queries_with_retries(client) 
            print()

    if "4" in stages_to_run:
        print(f"#########################################################")
        print(f"### 4. query after writing (delete pod after writing) ###")
        print(f"#########################################################")
        conn = get_conn(release_name)
        client = get_client(release_name)
        for pod in pods:
            if "frontend" in pod:
                continue
            print(f"Testing {pod}...")
            # first delete data to avoid duplicate vertices
            vertices = prepare_data()
            vertices_num = len(vertices)
            snapshot_id = delete_data(conn, vertices)
            # confirm the data has been deleted first
            assert conn.remote_flush(snapshot_id)
            before_count = get_statistics(client)
            # then insert data
            snapshot_id = insert_data(conn, vertices)
             # confirm the data has been inserted
            assert conn.remote_flush(snapshot_id)
            # test if we delete the pod...
            delete_and_watch(pod)
            after_count = get_statistics(client)
            if after_count - before_count == vertices_num:
                print(f"Data has been inserted successfully: previous count {before_count}, current count {after_count}, inserted vertices number {vertices_num}")
            else:
                raise Exception(f"Data has not been inserted successfully: previous count {before_count}, current count {after_count}, inserted vertices number {vertices_num}")
            print()
            

    if "5" in stages_to_run:
        print(f"#########################################################")
        print(f"### 5. query after writing (delete pod during writing) ##")
        print(f"#########################################################")
        conn = get_conn(release_name)
        client = get_client(release_name)
        for pod in pods:
            if "frontend" in pod:
                continue
            print(f"Testing {pod}...")
            # first delete data to avoid duplicate vertices
            vertices = prepare_data()
            vertices_num = len(vertices)
            snapshot_id = delete_data(conn, vertices)
            # confirm the data has been deleted first
            assert conn.remote_flush(snapshot_id)
            before_count = get_statistics(client)
            # then insert data
            snapshot_id = insert_data(conn, vertices)
            # directly delete the pod during writing
            delete_and_watch(pod)
            # confirm the status of whether data has been inserted
            data_flush_status = conn.remote_flush(snapshot_id)
            after_count = get_statistics(client)
            if not data_flush_status:
                print(f"data_flush_status is false, previous count {before_count}, current count {after_count}, inserted vertices number {vertices_num}")
            else:
                if after_count - before_count == vertices_num:
                    print(f"Data has been inserted successfully: previous count {before_count}, current count {after_count}, inserted vertices number {vertices_num}")
                else:
                    raise Exception(f"Data has not been inserted successfully: previous count {before_count}, current count {after_count}, inserted vertices number {vertices_num}")
            print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kubernetes failover testing script.')
    parser.add_argument('--instance_name', required=True, help='Name of the instance to target pods.')
    parser.add_argument('--namespace', default='kubetask', help='Kubernetes namespace to interact with.')
    parser.add_argument('--kubeconfig', default='~/.kube/config', help='Path to the kubeconfig file to use for CLI requests.')
    parser.add_argument('--stage', nargs='+', choices=['1', '2', '3', '4', '5', 'all'], default='all', help="Stage(s) of test to execute (1, 2, 3, 4, 5, or 'all').")
    
    args = parser.parse_args()
    main(args)
