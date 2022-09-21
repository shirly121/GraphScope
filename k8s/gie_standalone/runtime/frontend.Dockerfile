FROM centos:7.9.2009

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# RUNTIME

RUN yum install -y java-1.8.0-openjdk-devel

WORKDIR /home/graphscope
