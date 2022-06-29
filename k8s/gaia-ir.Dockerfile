FROM rust:1.60

# update source and dependence
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/http/https/g' /etc/apt/sources.list && \
    apt-get update -y

# set language and encoding
RUN apt-get -y install locales &&\
    sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen

# install cmake/clang/protobuf-compiler/rustfmt
RUN apt-get install -y cmake=3.18.4-2+deb11u1  && \
    apt-get install -y clang=1:11.0-51+nmu5 && \
    apt-get install -y protobuf-compiler=3.12.4-1 && \
    rustup component add rustfmt

# install Java Dependencies
RUN apt-get install -y openjdk-11-jdk=11.0.15+10-1~deb11u1 && \
    apt-get install -y maven=3.6.3-5

# git clone GraphScope to opt and compile
RUN cd /opt && \
    git clone https://github.com/longbinlai/GraphScope.git && \
    cd /opt/GraphScope/research/query_service/ir && \
    git fetch origin ir_catalog_dev:ir_catalog_dev && \
    git checkout ir_catalog_dev && \
    cargo build --release

# start RPC Server
RUN cd /opt/GraphScope/research/query_service/ir && \
    RUST_LOG=info ./target/release/start_rpc_server --config ./integrated/config/