#!/bin/bash

. kill.sh

# Initialize orderValue variable
orderValue=""

# Loop through arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --order) # Look for --order parameter
            orderValue="$2" # Assign the next argument as the value of --order
            shift # Move past the value
            ;;
    esac
    shift # Move to the next key-value pair or standalone flag
done

# Start GIE System
RUST_LOG=info DATA_PATH=${DATA}/imdb_bin ${GIE_HOME}/bin/start_rpc_server_on_imdb --config=${CONFIG}/engine &

sleep 2m

graph_schema=${GIE_HOME}/config/compiler/imdb_schema.yaml
physical_opt_config=proto
graph_planner_cbo_glogue_schema=${GIE_HOME}/config/compiler/imdb_statistics.json
graph_planner_opt=CBO

# Determine the CBO test to run based on orderValue
java \
  -cp ".:${GIE_HOME}/libs/*" \
  -Dgraph.schema=${graph_schema} \
  -Dgraph.physical.opt=${physical_opt_config} \
  -Dgraph.planner.opt=${graph_planner_opt} \
  -Dgraph.statistics=${graph_planner_cbo_glogue_schema} \
  -Dconfig=${CONFIG}/compiler/compiler.properties \
  -Dquery=${QUERY}/job \
  -Dorder=${orderValue} \
  org.apache.calcite.plan.volcano.LDBCTest
