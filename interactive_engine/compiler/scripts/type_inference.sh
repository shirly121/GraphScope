#!/bin/bash

. kill.sh

# Initialize optValue variable
optValue=""

# Loop through arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --opt) # Look for --opt parameter
            optValue="$2" # Assign the next argument as the value of --opt
            shift # Move past the value
            ;;
    esac
    shift # Move to the next key-value pair or standalone flag
done

# Start GIE System
RUST_LOG=info DATA_PATH=${DATA}/bi_30_bin_p1 ${GIE_HOME}/bin/start_rpc_server --config=${CONFIG}/engine &

sleep 2m

graph_schema=${GIE_HOME}/config/compiler/ldbc_schema.json
physical_opt_config=proto
graph_planner_cbo_glogue_schema=${GIE_HOME}/config/compiler/ldbc30_statistics.txt
graph_planner_opt=CBO

java \
  -cp ".:${GIE_HOME}/libs/*" \
  -Djna.library.path=${GIE_HOME}/libs \
  -Dgraph.schema=${graph_schema} \
  -Dphysical.opt.config=${physical_opt_config} \
  -Dgraph.planner.opt=${graph_planner_opt} \
  -Dgraph.planner.cbo.glogue.schema=${graph_planner_cbo_glogue_schema} \
  com.alibaba.graphscope.GraphServer ${CONFIG}/compiler/compiler.properties &

sleep 10s

# run type_inference test
java \
  -cp ".:${GIE_HOME}/libs/*" \
  -Dgraph.schema=${graph_schema} \
  -Dphysical.opt.config=${physical_opt_config} \
  -Dgraph.planner.opt=${graph_planner_opt} \
  -Dgraph.planner.cbo.glogue.schema=${graph_planner_cbo_glogue_schema} \
  -Dgraph.type.inference.enabled=$optValue \
  -Dconfig=${CONFIG}/compiler/compiler.properties \
  -Dquery=${QUERY} \
  org.apache.calcite.plan.volcano.TypeInferenceTest
