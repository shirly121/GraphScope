/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class GrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(GrpcClient.class);

    public static void main(String[] args) throws Exception {
        int threadNum = Integer.parseInt(args[0]);
        int queryNum = Integer.parseInt(args[1]);
        Configs configs = new Configs(args[2]);
        ChannelFetcher fetcher = new HostsRpcChannelFetcher(configs);
        RpcClient rpcClient = new RpcClient(fetcher.fetch());
        AtomicInteger id = new AtomicInteger(0);
        byte[] physicalBytes = FileUtils.readFileToByteArray(new File("physical_plan.bytes"));
        List<Thread> threads = Lists.newArrayList();
        AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < threadNum; ++i) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < queryNum; ++j) {
                    long queryId = id.getAndIncrement();
                    PegasusClient.JobRequest request =
                            PegasusClient.JobRequest.newBuilder()
                                    .setPlan(ByteString.copyFrom(physicalBytes))
                                    .build();
                    PegasusClient.JobConfig jobConfig =
                            PegasusClient.JobConfig.newBuilder()
                                    .setJobId(queryId)
                                    .setJobName("plan_" + queryId)
                                    .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(configs))
                                    .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(configs))
                                    .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(configs))
                                    .setBatchCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs))
                                    .setTimeLimit(3000000)
                                    .setAll(PegasusClient.Empty.newBuilder().build())
                                    .build();
                    request = request.toBuilder().setConf(jobConfig).build();
                    rpcClient.submit(request, new ResultProcessor() {
                        @Override
                        public void process(PegasusClient.JobResponse jobResponse) {
                        }

                        @Override
                        public void finish() {
                            logger.info("finish");
                            counter.getAndIncrement();
                        }

                        @Override
                        public void error(Status status) {
                            logger.error("error: {}", status);
                        }
                    }, 3000000);
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        while(counter.get() != threadNum * queryNum) {
            Thread.sleep(1000);
        }
        logger.info("all tasks are all finished");
    }
}
