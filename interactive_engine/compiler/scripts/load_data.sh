#!/bin/bash

mkdir -p ${DATA}
curl -o ${DATA}/bi_30_bin_p1.tar.gz -L "https://graphscope.oss-cn-beijing.aliyuncs.com/gopt_data/bi_30_bin_p1.tar.gz"
cd ${DATA} && tar xvzf bi_30_bin_p1.tar.gz
