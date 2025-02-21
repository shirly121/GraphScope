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

package com.alibaba.graphscope.common.ir.planner.tech;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.metric.MetricsTool;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class AblationTest {
    private GraphRelOptimizer optimizer;
    private Configs configs;
    private IrMeta irMeta;
    private GraphBuilder builder;
    private ExecutionClient client;
    private File logFile;

    private void createOptimizer(String rules) throws Exception {
        configs = new Configs(System.getProperty("conf", "conf/ir.compiler.properties"));
        configs.set("graph.planner.rules", rules);
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc_schema_exp_hierarchy.json",
                        "statistics/ldbc30_hierarchy_statistics.json",
                        optimizer);
        builder = Utils.mockGraphBuilder(optimizer, irMeta);
        logFile = new File(System.getProperty("log", "tech.log"));
        client =
                new RpcExecutionClient(
                        configs, new HostsRpcChannelFetcher(configs), new MetricsTool(configs));
    }

    // FilterMatchRule, FieldTrimRule, TopKPushDownRule, ExpandGetVFusionRule, LateProjectionRule,
    // IndexScanRule
    @Test
    public void ic_2_test() throws Exception {
        FileUtils.writeStringToFile(
                logFile,
                "************************Run IC 2 Query************************\n\n\n",
                StandardCharsets.UTF_8);
        List<String> params = Lists.newArrayList("123", "20120324", "45678");

        String indexed =
                "MATCH (p:PERSON)-[k:KNOWS]-(friend:PERSON)<-[h:HASCREATOR]-(message : POST |"
                        + " COMMENT) \n"
                        + "WHERE \n"
                        + "    elementId(p) = $3\n"
                        + "    AND h.creationDate <= $2 \n"
                        + "WITH \n"
                        + "    friend, \n"
                        + "    message \n"
                        + "ORDER BY \n"
                        + "    message.creationDate DESC, \n"
                        + "    message.id ASC LIMIT 20 \n"
                        + "Return \n"
                        + "    friend.id AS personId,\n"
                        + "    friend.firstName AS personFirstName,\n"
                        + "    friend.lastName AS personLastName, \n"
                        + "    message.id AS postOrCommentId,\n"
                        + "    message.content AS content,\n"
                        + "    message.imageFile AS imageFile,\n"
                        + "    message.creationDate AS postOrCommentCreationDate;";

        // all rules
        GraphPlanner.Summary all_plan =
                buildPhysical(
                        "all_plan",
                        indexed,
                        params,
                        false,
                        "FilterMatchRule, FieldTrimRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule");
        executePhysical(all_plan);

        String noIndexed =
                "MATCH (p:PERSON)-[k:KNOWS]-(friend:PERSON)<-[h:HASCREATOR]-(message : POST |"
                        + " COMMENT) \n"
                        + "WHERE \n"
                        + "    p.id = $1\n"
                        + "    AND h.creationDate <= $2 \n"
                        + "WITH \n"
                        + "    friend, \n"
                        + "    message \n"
                        + "ORDER BY \n"
                        + "    message.creationDate DESC, \n"
                        + "    message.id ASC LIMIT 20 \n"
                        + "Return \n"
                        + "    friend.id AS personId,\n"
                        + "    friend.firstName AS personFirstName,\n"
                        + "    friend.lastName AS personLastName, \n"
                        + "    message.id AS postOrCommentId,\n"
                        + "    message.content AS content,\n"
                        + "    message.imageFile AS imageFile,\n"
                        + "    message.creationDate AS postOrCommentCreationDate;";

        // remove IndexScanRule
        GraphPlanner.Summary no_index_plan =
                buildPhysical(
                        "no_index_plan",
                        noIndexed,
                        params,
                        false,
                        "FilterMatchRule, FieldTrimRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule");
        executePhysical(no_index_plan);

        // remove LateProjectionRule
        GraphPlanner.Summary no_late_plan =
                buildPhysical(
                        "no_late_plan",
                        noIndexed,
                        params,
                        true,
                        "FilterMatchRule, FieldTrimRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule");
        executePhysical(no_late_plan);

        // remove ExpandGetVFusionRule
        GraphPlanner.Summary no_fusion_plan =
                buildPhysical(
                        "no_fusion_plan",
                        noIndexed,
                        params,
                        true,
                        "FilterMatchRule, FieldTrimRule, ExtendIntersectRule");
        executePhysical(no_fusion_plan);

        String noTopK =
                "MATCH (p:PERSON)-[k:KNOWS]-(friend:PERSON)<-[h:HASCREATOR]-(message : POST |"
                        + " COMMENT) \n"
                        + "WHERE \n"
                        + "    p.id = $1\n"
                        + "    AND h.creationDate <= $2 \n"
                        + "WITH \n"
                        + "    friend, \n"
                        + "    message \n"
                        + "Return \n"
                        + "    friend.id AS personId,\n"
                        + "    friend.firstName AS personFirstName,\n"
                        + "    friend.lastName AS personLastName, \n"
                        + "    message.id AS postOrCommentId,\n"
                        + "    message.content AS content,\n"
                        + "    message.imageFile AS imageFile,\n"
                        + "    message.creationDate AS postOrCommentCreationDate\n"
                        + "ORDER BY \n"
                        + "    postOrCommentCreationDate DESC, \n"
                        + "    postOrCommentId ASC LIMIT 20 \n";

        // remove TopKPushDownRule
        GraphPlanner.Summary no_topK_plan =
                buildPhysical(
                        "no_topK_plan",
                        noTopK,
                        params,
                        true,
                        "FilterMatchRule, FieldTrimRule, ExtendIntersectRule");
        executePhysical(no_topK_plan);

        // remove FieldTrimRule
        GraphPlanner.Summary no_trim_plan =
                buildPhysical(
                        "no_trim_plan",
                        noTopK,
                        params,
                        true,
                        "FilterMatchRule, ExtendIntersectRule");
        executePhysical(no_trim_plan);

        // remove FilterMatchRule
        GraphPlanner.Summary no_filter_plan =
                buildPhysical("no_filter_plan", noTopK, params, true, "ExtendIntersectRule");
        executePhysical(no_filter_plan);

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8);
    }

    // FilterIntoMatchRule, CommonPatternReuseRule, ExpandGetVFusionRule, DegreeFusionRule
    @Test
    public void bi_5_test() throws Exception {
        FileUtils.writeStringToFile(
                logFile,
                "************************Run BI 5 Query************************\n\n\n",
                StandardCharsets.UTF_8);
        String template =
                "Match (tag:TAG)<-[:HASTAG]-(message:POST|COMMENT)\n"
                        + "Where tag.name = $tag\n"
                        + "WITH DISTINCT message\n"
                        + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:PERSON)\n"
                        + "WITH message, count(liker) as likeCount\n"
                        + "OPTIONAL MATCH (message)<-[:REPLYOF]-(comment:COMMENT)\n"
                        + "WITH message, likeCount, count(comment) as replyCount\n"
                        + "MATCH (message)-[:HASCREATOR]->(person:PERSON)\n"
                        + "Return \n"
                        + "  person.id AS id,\n"
                        + "  sum(replyCount) as replyCount,\n"
                        + "  sum(likeCount) as likeCount,\n"
                        + "  count(message) as messageCount\n";
        GraphPlanner.Summary all_plan =
                buildPhysical(
                        "all_plan",
                        template,
                        Lists.newArrayList(),
                        false,
                        "FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule, DegreeFusionRule");
        executePhysical(all_plan);

        // remove DegreeFusionRule
        GraphPlanner.Summary no_degree_plan =
                buildPhysical(
                        "no_degree_plan",
                        template,
                        Lists.newArrayList(),
                        false,
                        "FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule");
        executePhysical(no_degree_plan);

        // remove ExpandGetVFusionRule
        GraphPlanner.Summary no_fusion_plan =
                buildPhysical(
                        "no_fusion_plan",
                        template,
                        Lists.newArrayList(),
                        false,
                        "FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule");
        executePhysical(no_fusion_plan);

        // remove FlatJoinToExpandRule
        GraphPlanner.Summary no_flat_plan =
                buildPhysical(
                        "no_flat_plan",
                        template,
                        Lists.newArrayList(),
                        false,
                        "FilterMatchRule, ExtendIntersectRule");
        executePhysical(no_flat_plan);

        // remove FilterIntoMatchRule
        GraphPlanner.Summary no_filter_plan =
                buildPhysical(
                        "no_filter_plan",
                        template,
                        Lists.newArrayList(),
                        false,
                        "ExtendIntersectRule");
        executePhysical(no_filter_plan);

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8);
    }

    // ManyToOneIndexRule
    @Test
    public void advanced_index_test() {}

    private String getQuery(String template, List<String> parameters) {
        for (int i = 1; i <= parameters.size(); ++i) {
            template = template.replace("$" + i, parameters.get(i - 1));
        }
        return template;
    }

    private GraphPlanner.Summary buildPhysical(
            String name, String template, List<String> params, boolean prefetch, String rules)
            throws Exception {
        createOptimizer(rules);
        String query = getQuery(template, params);
        RelNode relNode = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode optimized = optimizer.optimize(relNode, new GraphIOProcessor(builder, irMeta));
        GraphRelProtoPhysicalBuilder builder1 =
                new GraphRelProtoPhysicalBuilder(
                        configs, irMeta, new LogicalPlan(optimized), false, prefetch);
        FileUtils.writeStringToFile(
                logFile, name + ": \n" + optimized.explain() + "\n", StandardCharsets.UTF_8);
        return new GraphPlanner.Summary(new LogicalPlan(optimized), builder1.build());
    }

    private void executePhysical(GraphPlanner.Summary plan) throws Exception {
        try {
            BigInteger queryId = new BigInteger(String.valueOf(UUID.randomUUID().hashCode()));
            String queryName = "job" + queryId;
            StreamIterator<IrResult.Record> resultIterator = new StreamIterator<>();
            long starTime = System.currentTimeMillis();
            client.submit(
                    new ExecutionRequest(
                            queryId, queryName, plan.getLogicalPlan(), plan.getPhysicalPlan()),
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
                    new QueryTimeoutConfig(FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs)),
                    new QueryLogger("", queryId));
            StringBuilder resultBuilder = new StringBuilder();
            while (resultIterator.hasNext()) {
                resultBuilder.append(resultIterator.next());
            }
            long elapsed = System.currentTimeMillis() - starTime;
            FileUtils.writeStringToFile(
                    logFile, "execution time is [" + elapsed + "] ms", StandardCharsets.UTF_8);
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile,
                    "execution error is " + e.getMessage().substring(0, 50),
                    StandardCharsets.UTF_8);
        }
    }
}
