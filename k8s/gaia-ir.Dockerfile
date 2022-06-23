FROM rust:1.61.0

# update source and dependence
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/http/https/g' /etc/apt/sources.list && \
    apt-get update -y

# install cmake/clang/protobuf-compiler/rustfmt
RUN apt-get install -y cmake=3.18.4-2+deb11u1  && \
    apt-get install -y clang=1:11.0-51+nmu5 && \
    apt-get install -y protobuf-compiler=3.12.4-1 && \
    rustup component add rustfmt

# git clone GraphScope to opt
RUN cd /opt && \
    git clone https://github.com/longbinlai/GraphScope.git