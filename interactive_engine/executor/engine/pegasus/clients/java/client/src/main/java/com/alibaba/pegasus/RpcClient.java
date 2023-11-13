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

package com.alibaba.pegasus;

import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.JobServiceGrpc;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobRequest;
import com.alibaba.pegasus.service.protocol.PegasusClient.JobResponse;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);
    private final List<RpcChannel> channels;
//    private final List<JobServiceStub> serviceStubs;
    private final List<JobServiceGrpc.JobServiceBlockingStub> serviceBlockingStubs;

    public RpcClient(List<RpcChannel> channels) {
        this.channels = Objects.requireNonNull(channels);
//        this.serviceStubs =
//                channels.stream()
//                        .map(k -> JobServiceGrpc.newStub(k.getChannel()))
//                        .collect(Collectors.toList());
        this.serviceBlockingStubs = channels.stream().map(k -> JobServiceGrpc.newBlockingStub(k.getChannel()).withDeadlineAfter(3000000, TimeUnit.MILLISECONDS)).collect(Collectors.toList());
    }

    public void submit(JobRequest jobRequest, ResultProcessor processor, long rpcTimeoutMS) {
//        AtomicInteger counter = new AtomicInteger(this.channels.size());
//        AtomicBoolean finished = new AtomicBoolean(false);
//        serviceStubs.forEach(
//                asyncStub -> {
//                    asyncStub
//                            .withDeadlineAfter(rpcTimeoutMS, TimeUnit.MILLISECONDS)
//                            .submit(
//                                    jobRequest,
//                                    new JobResponseObserver(processor, finished, counter));
//                });
        logger.info("submit job sync");
        this.serviceBlockingStubs.forEach(blockingStub -> {
            try {
                Iterator<JobResponse> response = blockingStub.submit(jobRequest);
                while (response.hasNext()) {
                    processor.process(response.next());
                }
                processor.finish();
            } catch (Exception e) {
                processor.error(Status.fromThrowable(e));
            }
        });
    }

    public void shutdown() throws InterruptedException {
        for (RpcChannel rpcChannel : channels) {
            rpcChannel.shutdown();
        }
    }

    private static class JobResponseObserver implements StreamObserver<JobResponse> {
        private final ResultProcessor processor;
        private final AtomicBoolean finished;
        private final AtomicInteger counter;

        public JobResponseObserver(
                ResultProcessor processor, AtomicBoolean finished, AtomicInteger counter) {
            this.processor = processor;
            this.finished = finished;
            this.counter = counter;
        }

        @Override
        public void onNext(JobResponse jobResponse) {
            if (finished.get()) {
                return;
            }
            processor.process(jobResponse);
        }

        @Override
        public void onError(Throwable throwable) {
            if (finished.getAndSet(true)) {
                return;
            }
            Status status = Status.fromThrowable(throwable);
            logger.error("get job response error: {}", status);
            processor.error(status);
        }

        @Override
        public void onCompleted() {
            logger.info("finish get job response from one server");
            if (counter.decrementAndGet() == 0) {
                logger.info("finish get job response from all servers");
                processor.finish();
            }
        }
    }
}
