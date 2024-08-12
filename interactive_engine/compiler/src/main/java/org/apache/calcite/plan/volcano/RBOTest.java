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

import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class RBOTest {
    public static void main(String[] args) throws Exception {
        Test test = new Test();
        test.Q_R_1();
        test.Q_R_2();
        test.Q_R_3();
        test.Q_R_4();
        test.Q_R_5();
        test.Q_R_6();
    }

    private static class Test {
        private final Configs configs;
        private final String opt;
        private final IrMeta irMeta;
        private final RpcExecutionClient executionClient;
        private final File log;
        private final String queryDir;

        public Test() throws Exception {
            configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
            opt = System.getProperty("opt", "without");
            irMeta = new StaticIrMetaFetcher(new LocalIrMetaReader(configs), null).fetch().get();
            executionClient = new RpcExecutionClient(configs, new HostsRpcChannelFetcher(configs));
            log = new File("rbo_result.txt");
            if (log.exists()) {
                log.delete();
            }
            log.createNewFile();
            queryDir = System.getProperty("query", "gopt");
        }

        public void Q_R_1() throws Exception {
            String physicalJson;
            if (opt.equals("with")) {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_1_with_opt"), StandardCharsets.UTF_8);
            } else {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_1_without_opt"), StandardCharsets.UTF_8);
            }
            long startTime = System.currentTimeMillis();
            GraphAlgebraPhysical.PhysicalPlan.Builder builder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            JsonFormat.parser().merge(physicalJson, builder);
            PhysicalPlan physicalPlan =
                    new PhysicalPlan(builder.build().toByteArray(), physicalJson);
            ExecutionRequest request =
                    new ExecutionRequest(BigInteger.valueOf(1), "Q_R_1", null, physicalPlan);
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_1", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_2() throws Exception {
            String physicalJson;
            if (opt.equals("with")) {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_2_with_opt"), StandardCharsets.UTF_8);
            } else {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_2_without_opt"), StandardCharsets.UTF_8);
            }
            long startTime = System.currentTimeMillis();
            GraphAlgebraPhysical.PhysicalPlan.Builder builder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            JsonFormat.parser().merge(physicalJson, builder);
            PhysicalPlan physicalPlan =
                    new PhysicalPlan(builder.build().toByteArray(), physicalJson);
            ExecutionRequest request =
                    new ExecutionRequest(BigInteger.valueOf(2), "Q_R_2", null, physicalPlan);
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_2", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_3() throws Exception {
            if (opt.equals("with")) {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                + " ExtendIntersectRule, ExpandGetVFusionRule");
            } else {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                + " ExtendIntersectRule");
            }
            GraphPlanner planner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String q) ->
                                    new LogicalPlanVisitor(builder, irMeta)
                                            .visit(new CypherAntlr4Parser().parse(q)));
            String query =
                    FileUtils.readFileToString(
                            new File(queryDir + "/Q_R_3"), StandardCharsets.UTF_8);
            long startTime = System.currentTimeMillis();
            GraphPlanner.Summary summary = planner.instance(query, irMeta).plan();
            ExecutionRequest request =
                    new ExecutionRequest(
                            BigInteger.valueOf(3),
                            "Q_R_3",
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_3", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_4() throws Exception {
            if (opt.equals("with")) {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                + " ExtendIntersectRule, ExpandGetVFusionRule");
            } else {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                + " ExtendIntersectRule");
            }
            GraphPlanner planner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String q) ->
                                    new LogicalPlanVisitor(builder, irMeta)
                                            .visit(new CypherAntlr4Parser().parse(q)));
            String query =
                    FileUtils.readFileToString(
                            new File(queryDir + "/Q_R_4"), StandardCharsets.UTF_8);
            long startTime = System.currentTimeMillis();
            GraphPlanner.Summary summary = planner.instance(query, irMeta).plan();
            ExecutionRequest request =
                    new ExecutionRequest(
                            BigInteger.valueOf(4),
                            "Q_R_4",
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_4", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_5() throws Exception {
            if (opt.equals("with")) {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                + " ExtendIntersectRule, ExpandGetVFusionRule");
            } else {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, ExtendIntersectRule, ExpandGetVFusionRule");
            }
            GraphPlanner planner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String q) ->
                                    new LogicalPlanVisitor(builder, irMeta)
                                            .visit(new CypherAntlr4Parser().parse(q)));
            String query =
                    FileUtils.readFileToString(
                            new File(queryDir + "/Q_R_5"), StandardCharsets.UTF_8);
            long startTime = System.currentTimeMillis();
            GraphPlanner.Summary summary = planner.instance(query, irMeta).plan();
            ExecutionRequest request =
                    new ExecutionRequest(
                            BigInteger.valueOf(5),
                            "Q_R_5",
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_5", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_6() throws Exception {
            if (opt.equals("with")) {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, FilterIntoJoinRule, FilterMatchRule,"
                                + " ExtendIntersectRule, ExpandGetVFusionRule");
            } else {
                configs.set(
                        PlannerConfig.GRAPH_PLANNER_RULES.getKey(),
                        "NotMatchToAntiJoinRule, ExtendIntersectRule, ExpandGetVFusionRule");
            }
            GraphPlanner planner =
                    new GraphPlanner(
                            configs,
                            (GraphBuilder builder, IrMeta irMeta, String q) ->
                                    new LogicalPlanVisitor(builder, irMeta)
                                            .visit(new CypherAntlr4Parser().parse(q)));
            String query =
                    FileUtils.readFileToString(
                            new File(queryDir + "/Q_R_6"), StandardCharsets.UTF_8);
            long startTime = System.currentTimeMillis();
            GraphPlanner.Summary summary = planner.instance(query, irMeta).plan();
            ExecutionRequest request =
                    new ExecutionRequest(
                            BigInteger.valueOf(6),
                            "Q_R_6",
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_6", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_7() throws Exception {
            String physicalJson;
            if (opt.equals("with")) {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_7_with_opt"), StandardCharsets.UTF_8);
            } else {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_7_without_opt"), StandardCharsets.UTF_8);
            }
            long startTime = System.currentTimeMillis();
            GraphAlgebraPhysical.PhysicalPlan.Builder builder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            JsonFormat.parser().merge(physicalJson, builder);
            PhysicalPlan physicalPlan =
                    new PhysicalPlan(builder.build().toByteArray(), physicalJson);
            ExecutionRequest request =
                    new ExecutionRequest(BigInteger.valueOf(7), "Q_R_7", null, physicalPlan);
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_1", elapsedTime),
                    "UTF-8",
                    true);
        }

        public void Q_R_8() throws Exception {
            String physicalJson;
            if (opt.equals("with")) {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_8_with_opt"), StandardCharsets.UTF_8);
            } else {
                physicalJson =
                        FileUtils.readFileToString(
                                new File(queryDir + "/Q_R_8_without_opt"), StandardCharsets.UTF_8);
            }
            long startTime = System.currentTimeMillis();
            GraphAlgebraPhysical.PhysicalPlan.Builder builder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            JsonFormat.parser().merge(physicalJson, builder);
            PhysicalPlan physicalPlan =
                    new PhysicalPlan(builder.build().toByteArray(), physicalJson);
            ExecutionRequest request =
                    new ExecutionRequest(BigInteger.valueOf(8), "Q_R_8", null, physicalPlan);
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
                    String.format("query: [%s], latency: [%d] ms\n", "Q_R_1", elapsedTime),
                    "UTF-8",
                    true);
        }
    }
}
