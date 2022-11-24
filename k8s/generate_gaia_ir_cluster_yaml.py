server_size = 8
worker_num = 2
scale=1
catalog_capacity = 10
graph_capacity = 10
compiler_port = 8182
compiler_node_port = 31812
compiler_image = "gaia-ir-compiler:v1.0"
compiler_node = "k69d09198.eu95sqa"
rpc_port = 1234
partition_port = 11234
rpc_image = "gaia-ir-rpc:v1.0"
schema_path = "../executor/ir/core/resource/ldbc_schema_edit.json"
local_graph_path = "/apsara/yufan/Graphs/"
local_catalog_path = "/home/admin/yufan/Catalogs/"
container_graph_path = "/opt/Graphs/"
container_catalog_path = "/opt/Catalogs/"
catalog_name = "several_patterns_catalog"
rpc_node_id_map = {0:"j66d16351.sqa.eu95", 1:"j66d16346.sqa.eu95", 2:"j66d16343.sqa.eu95", 3:"j66d16342.sqa.eu95", 4:"j66d14415.sqa.eu95", 5:"j66d14410.sqa.eu95", 6:"j66d14409.sqa.eu95", 7:"j66d14408.sqa.eu95"}

persistent_volume_template = '''\
apiVersion: v1
kind: PersistentVolume
metadata:
  name: {}
spec:
  capacity:
    storage: {}Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  hostPath:
    path: {}
  claimRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    name: {}
    namespace: default
'''

persistent_volume_claim_template = '''\
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {}
spec:
  storageClassName: ""
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {}Gi
'''

compiler_sercvice_template = '''\
apiVersion: v1
kind: Service
metadata:
  name: gaia-ir-compiler-cs
  labels:
    app: gaia-ir-compiler
spec:
  type: LoadBalancer
  ports:
    - port: {}
      targetPort: {}
      nodePort: {}
      name: listen-query
  selector:
    app: gaia-ir-compiler
'''

rpc_service_template = '''\
apiVersion: v1
kind: Service
metadata:
  name: gaia-ir-rpc-hs
  labels:
    app: gaia-ir-rpc
spec:
  ports:
    - port: {}
      name: rpc
    - port: {}
      name: repartition
  clusterIP: None
  selector:
    app: gaia-ir-rpc
'''

compiler_pod_template = '''\
apiVersion: v1
kind: Pod
metadata:
  name: gaia-ir-compiler
  labels:
    app: gaia-ir-compiler
spec:
  volumes:
    - name: catalog
      persistentVolumeClaim:
        claimName: catalog-pvc
  containers:
  - name: gaia-ir-compiler
    image: {}
    imagePullPolicy: IfNotPresent
    ports:
      - containerPort: {}
        name: listen-query
    env:
      - name: WORKER_NUM
        value: "{}"
      - name: TIMEOUT
        value: "240000"
      - name: BATCH_SIZE
        value: "1024"
      - name: OUTPUT_CAPACITY
        value: "16"
      - name: GRAPH_SCHEMA
        value: "{}"
      - name: CATALOG_PATH
        value: "{}"
      - name: SERVERSSIZE
        value: "{}"
      - name: DNS_NAME_PREFIX_STORE
        value: "gaia-ir-rpc-{{}}.gaia-ir-rpc-hs.default.svc.cluster.local"
      - name: GAIA_RPC_PORT
        value: "{}"
    command:
      - sh
      - -c
      - "cd /opt/GraphScope/interactive_engine/compiler && \\
        ./set_properties.sh && make run graph.schema:=$GRAPH_SCHEMA"
    volumeMounts:
      - name: catalog
        mountPath: /opt/Catalogs
  nodeName: {}
  hostname: gaia-ir-compiler
'''

rpc_pod_template = '''\
apiVersion: v1
kind: Pod
metadata:
  name: gaia-ir-rpc-{}
  labels:
    app: gaia-ir-rpc
spec:
  volumes:
    - name: graph-store{}
      persistentVolumeClaim:
        claimName: graph-store-pvc-{}
  containers:
  - name: gaia-ir-rpc
    image: {}
    imagePullPolicy: IfNotPresent
    ports:
      - containerPort: {}
        name: rpc
      - containerPort: {}
        name: repartition
    env:
      - name: DATA_PATH
        value: "{}"  
      - name: RUST_LOG
        value: "info"  
      - name: SERVERSSIZE
        value: "{}"
      - name: DNS_NAME_PREFIX_STORE
        value: "gaia-ir-rpc-{{}}.gaia-ir-rpc-hs.default.svc.cluster.local"
      - name: GAIA_RPC_PORT
        value: "{}"
      - name: GAIA_ENGINE_PORT
        value: "{}"
    command:
      - sh
      - -c
      -  "/opt/GraphScope/interactive_engine/executor/ir/target/release/start_rpc_server_k8s"
    volumeMounts:
      - name: graph-store{}
        mountPath: {}
  nodeName: {}
  hostname: gaia-ir-rpc-{}
  subdomain: gaia-ir-rpc-hs
'''

def generate_compiler_pv():
    compiler_pv = persistent_volume_template.format("catalog", catalog_capacity, local_catalog_path, "catalog-pvc")
    return compiler_pv

def generate_compiler_pvc():
    compiler_pvc = persistent_volume_claim_template.format("catalog-pvc", catalog_capacity)
    return compiler_pvc

def generate_compiler_service():
    compiler_service = compiler_sercvice_template.format(compiler_port, compiler_port, compiler_node_port)
    return compiler_service

def generate_compiler_pod():
    compiler_pod = compiler_pod_template.format(compiler_image, compiler_port, worker_num, schema_path, container_catalog_path+catalog_name, server_size, rpc_port, compiler_node);
    return compiler_pod

def generate_rpc_pv(id):
    rpc_pv_name = "graph-store{}".format(id);
    rpc_pvc_name = "graph-store-pvc-{}".format(id);
    rpc_pv = persistent_volume_template.format(rpc_pv_name, graph_capacity, local_graph_path, rpc_pvc_name)
    return rpc_pv

def generate_rpc_pvc(id):
    rpc_pvc_name = "graph-store-pvc-{}".format(id);
    rpc_pvc = persistent_volume_claim_template.format(rpc_pvc_name, graph_capacity);
    return rpc_pvc

def generate_rpc_service():
    rpc_sercive = rpc_service_template.format(rpc_port, partition_port)
    return rpc_sercive

def generate_rpc_pod(id):
    graph_path = container_graph_path + "scale_{}/".format(scale) + "bin_p{}".format(server_size)
    rpc_pod = rpc_pod_template.format(id, id, id, rpc_image, rpc_port, partition_port, graph_path, server_size, rpc_port, partition_port, id, container_graph_path, rpc_node_id_map[id], id)
    return rpc_pod

def generate_gaia_ir_cluster():
    yaml_str = generate_compiler_service() + "---\n"
    yaml_str += generate_compiler_pv() + "---\n"
    yaml_str += generate_compiler_pvc() + "---\n"
    yaml_str += generate_compiler_pod() + "---\n"
    yaml_str += generate_rpc_service() + "---\n"
    for i in range(0, server_size):
        yaml_str += generate_rpc_pv(i) + "---\n"
        yaml_str += generate_rpc_pvc(i) + "---\n"
        yaml_str += generate_rpc_pod(i) + "---\n"
    return yaml_str[:-5]

print(generate_gaia_ir_cluster())