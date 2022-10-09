#!/bin/bash
worker_num="pegasus.worker.num: $WORKER_NUM";

timeout="pegasus.timeout: $TIMEOUT"

batch_size="pegasus.batch.size: $BATCH_SIZE";

output_capacity="pegasus.output.capacity: $OUTPUT_CAPACITY";

hosts="pegasus.hosts: gaia-ir-rpc-0.gaia-ir-rpc-hs.default.svc.cluster.local:1234"

count=1;
while (($count<$SERVERSSIZE))
do
    hosts+=", gaia-ir-rpc-$count.gaia-ir-rpc-hs.default.svc.cluster.local:1234"
    let "count++"
done

server_num="pegasus.server.num: $SERVERSSIZE"

graph_schema="graph.schema: $GRAPH_SCHEMA"

properties="$worker_num\n$timeout\n$batch_size\n$output_capacity\n$hosts\n$server_num\n$graph_schema"

echo -e $properties > ./conf/ir.compiler.properties