FROM gaia-ir

# set environment
ENV RUST_LOG info

# start RPC Server
CMD ["/opt/GraphScope/research/query_service/ir/target/release/start_rpc_server", "--config", "/opt/GraphScope/research/query_service/ir/integrated/config/"]