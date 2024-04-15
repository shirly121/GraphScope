/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.calcite.plan.volcano;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Callable;

public class TypeInferenceTest {

    public static void main(String[] args) throws Exception {
        Test test = new Test();
        test.Q_T_1();
        // test.Q_T_2();
        test.Q_T_3();
        test.Q_T_4();
        test.Q_T_5();
        test.close();
    }

    public static class Test {
        private final Configs configs;
        private final boolean withOpt;
        private final Cluster cluster;
        private final Client client;
        private final File log;
        private final GraphPlanner planner;
        private final IrMetaFetcher metaFetcher;
        private final ExecutionClient executionClient;
        private final String queryDir;

        public Test() throws Exception {
            configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
            withOpt = FrontendConfig.GRAPH_TYPE_INFERENCE_ENABLED.get(configs);
            cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
            client = cluster.connect();
            log = new File("type_inference_result.txt");
            if (log.exists()) {
                log.delete();
            }
            log.createNewFile();
            planner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String q) ->
                                    new LogicalPlanVisitor(builder, irMeta)
                                            .visit(new CypherAntlr4Parser().parse(q)));
            metaFetcher = new ExperimentalMetaFetcher(new LocalMetaDataReader(configs));
            executionClient = new RpcExecutionClient(configs, new HostsRpcChannelFetcher(configs));
            queryDir = System.getProperty("query", "gopt");
        }

        public void Q_T_1() throws Exception {
            String query;
            if (withOpt) {
                query =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_T_1_with_opt"), StandardCharsets.UTF_8);
            } else {
                query =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_T_1_without_opt"), StandardCharsets.UTF_8);
            }
            FileUtils.writeStringToFile(
                    log,
                    String.format(
                            "query: [%s], latency: [%d] ms\n",
                            "Q_T_1", getElapsedTime(() -> client.submit(query).all())),
                    "UTF-8",
                    true);
        }

        public void Q_T_2() throws Exception {
            String query;
            if (withOpt) {
                query =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_T_2_with_opt"), StandardCharsets.UTF_8);
            } else {
                query =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_T_2_without_opt"), StandardCharsets.UTF_8);
            }
            FileUtils.writeStringToFile(
                    log,
                    String.format(
                            "query: [%s], latency: [%d] ms\n",
                            "Q_T_2", getElapsedTime(() -> client.submit(query).all())),
                    "UTF-8",
                    true);
        }

        public void Q_T_3() throws Exception {
            String query;
            if (withOpt) {
                query =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_T_3_with_opt"), StandardCharsets.UTF_8);
            } else {
                query =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_T_3_without_opt"), StandardCharsets.UTF_8);
            }
            FileUtils.writeStringToFile(
                    log,
                    String.format(
                            "query: [%s], latency: [%d] ms\n",
                            "Q_T_3", getElapsedTime(() -> client.submit(query).all())),
                    "UTF-8",
                    true);
        }

        public void Q_T_4() throws Exception {
            String query =
                    FileUtils.readFileToString(
                            new File(queryDir + "/Q_T_4"), StandardCharsets.UTF_8);
            long startTime = System.currentTimeMillis();
            GraphPlanner.Summary summary =
                    planner.instance(query, metaFetcher.fetch().get()).plan();
            ExecutionRequest request =
                    new ExecutionRequest(
                            UUID.randomUUID().hashCode(),
                            "Q_T_4",
                            summary.getLogicalPlan(),
                            summary.getPhysicalPlan());
            StreamIterator<IrResult.Record> resultIterator = new StreamIterator<>();
            executionClient.submit(
                    request,
                    new ExecutionResponseListener() {
                        @Override
                        public void onNext(IrResult.Record record) {
                            try {
                                resultIterator.putData(record);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onCompleted() {
                            try {
                                resultIterator.finish();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            resultIterator.fail(t);
                        }
                    },
                    new QueryTimeoutConfig(FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs)));
            StringBuilder resultBuilder = new StringBuilder();
            while (resultIterator.hasNext()) {
                resultBuilder.append(resultIterator.next());
                // resultIterator.next();
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    log,
                    String.format("query: [%s], latency: [%d] ms\n", "Q_T_4", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_T_5() throws Exception {
            String query =
                    FileUtils.readFileToString(
                            new File(queryDir + "/Q_T_5"), StandardCharsets.UTF_8);
            long startTime = System.currentTimeMillis();
            GraphPlanner.Summary summary =
                    planner.instance(query, metaFetcher.fetch().get()).plan();
            ExecutionRequest request =
                    new ExecutionRequest(
                            UUID.randomUUID().hashCode(),
                            "Q_T_5",
                            summary.getLogicalPlan(),
                            summary.getPhysicalPlan());
            StreamIterator<IrResult.Record> resultIterator = new StreamIterator<>();
            executionClient.submit(
                    request,
                    new ExecutionResponseListener() {
                        @Override
                        public void onNext(IrResult.Record record) {
                            try {
                                resultIterator.putData(record);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onCompleted() {
                            try {
                                resultIterator.finish();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            resultIterator.fail(t);
                        }
                    },
                    new QueryTimeoutConfig(FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs)));
            StringBuilder resultBuilder = new StringBuilder();
            while (resultIterator.hasNext()) {
                resultBuilder.append(resultIterator.next());
                // resultIterator.next();
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    log,
                    String.format("query: [%s], latency: [%d] ms\n", "Q_T_5", elapsedTime),
                    "UTF-8",
                    true);
        }

        private long getElapsedTime(Callable call) throws Exception {
            long startTime = System.currentTimeMillis();
            call.call();
            return System.currentTimeMillis() - startTime;
        }

        public void close() {
            if (cluster != null) {
                cluster.close();
            }
            if (client != null) {
                client.close();
            }
        }
    }
}
