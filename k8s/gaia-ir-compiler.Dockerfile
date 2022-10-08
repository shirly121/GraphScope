FROM gaia-ir

# build compiler
RUN cd /opt/GraphScope/research/query_service/ir/compiler && \
    make build && \
    cargo build --release

CMD cd /opt/GraphScope/research/query_service/ir/compiler && \
    make run graph.schema:=../executor/ir/core/resource/modern_schema.json