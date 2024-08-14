# Interactive engine which uses experimental storage
FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:v0.16.4 AS builder

COPY --chown=graphscope:graphscope . /home/graphscope/GraphScope

RUN cd /home/graphscope/GraphScope/interactive_engine/compiler \
    && . /home/graphscope/.graphscope_env \
    && make build \
    && make build_imdb

############### RUNTIME: frontend && executor #######################
FROM ubuntu:22.04 AS experimental

ENV DEBIAN_FRONTEND=noninteractive

COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/target/libs /home/graphscope/GIE/libs
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/target/compiler-0.0.1-SNAPSHOT.jar /home/graphscope/GIE/libs
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/conf/ir.compiler.properties /home/graphscope/GIE/config/compiler/compiler.properties
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/core/resource/ldbc_schema.json /home/graphscope/GIE/config/compiler/ldbc_schema.json
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/core/resource/ldbc_schema_exp_hierarchy.json /home/graphscope/GIE/config/compiler/ldbc_schema_hierarchy.json
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/core/resource/imdb_schema_general.yaml /home/graphscope/GIE/config/compiler/imdb_schema.yaml
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/src/test/resources/statistics/ldbc30_statistics.json /home/graphscope/GIE/config/compiler/ldbc30_statistics.json
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/src/test/resources/statistics/ldbc30_hierarchy_statistics.json /home/graphscope/GIE/config/compiler/ldbc30_hierarchy_statistics.json
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/src/test/resources/statistics/imdb_statistics.json /home/graphscope/GIE/config/compiler/imdb_statistics.json
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/integrated/config /home/graphscope/GIE/config/engine
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/scripts /home/graphscope/GIE/scripts
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/compiler/query /home/graphscope/GIE/query
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/libir_core.so /home/graphscope/GIE/libs
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/start_rpc_server /home/graphscope/GIE/bin/start_rpc_server
COPY --from=builder /home/graphscope/GraphScope/interactive_engine/executor/ir/target/release/start_rpc_server_on_imdb /home/graphscope/GIE/bin/start_rpc_server_on_imdb

RUN apt-get update -y && \
    apt-get install -y default-jdk tzdata && \
    apt-get install -y curl && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN chmod a+wrx /tmp

RUN useradd -m graphscope -u 1001 \
    && echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphscope
WORKDIR /home/graphscope

ENV PATH=${PATH}:/home/graphscope/.local/bin
ENV GIE_HOME=/home/graphscope/GIE
ENV CONFIG=${GIE_HOME}/config
ENV DATA=/tmp/data
ENV QUERY=${GIE_HOME}/query