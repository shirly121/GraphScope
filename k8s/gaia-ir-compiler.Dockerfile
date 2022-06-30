FROM gaia-ir

# build compiler
RUN cd /opt/GraphScope/research/query_service/ir/compiler && \
    make build && \
    cargo build --release

CMD cd /opt/GraphScope/research/query_service/ir/compiler && \
    make run