FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:v0.8.2 AS builder
ADD . /home/GraphScope
ARG profile=release
RUN sudo chown -R graphscope:graphscope /home/GraphScope
RUN source $HOME/.bashrc \
    && cd /home/GraphScope/interactive_engine \
    && mvn clean package -DskipTests -Drust.compile.mode=$profile -P graphscope,graphscope-assembly --quiet

# RUNTIME: frontend
FROM registry.cn-hongkong.aliyuncs.com/graphscope/frontend-runtime:latest AS frontend
COPY --from=builder /home/GraphScope/interactive_engine/assembly/target/graphscope.tar.gz /home/GraphScope/graphscope.tar.gz
RUN cd /home/GraphScope && tar xf graphscope.tar.gz -C /opt

# RUNTIME: executor
FROM registry.cn-hongkong.aliyuncs.com/graphscope/executor-runtime:latest AS executor
COPY --from=builder /home/GraphScope/interactive_engine/assembly/target/graphscope.tar.gz /home/GraphScope/graphscope.tar.gz
RUN cd /home/GraphScope && tar xf graphscope.tar.gz -C /opt
# todo: copy vineyard binaries
COPY --from=builder /opt/vineyard /opt/vineyard
