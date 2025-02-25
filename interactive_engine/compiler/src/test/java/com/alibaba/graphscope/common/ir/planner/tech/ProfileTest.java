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
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class ProfileTest {
    private GraphRelOptimizer optimizer;
    private Configs configs;
    private IrMeta irMeta;
    private GraphBuilder builder;
    private static ExecutionClient client;
    private static File logFile;

    @BeforeClass
    public static void setup() throws Exception {
        logFile = new File(System.getProperty("log", "profile.log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        Configs configs1 = new Configs(System.getProperty("conf", "conf/ir.compiler.2.properties"));
        client =
                new RpcExecutionClient(
                        configs1, new HostsRpcChannelFetcher(configs1), new MetricsTool(configs1));
    }

    private void createOptimizer(String rules) throws Exception {
        configs = new Configs(System.getProperty("conf", "conf/ir.compiler.properties"));
        configs.set("graph.planner.rules", rules);
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                new StaticIrMetaFetcher(new LocalIrMetaReader(configs), ImmutableList.of(optimizer))
                        .fetch()
                        .get();
        builder = Utils.mockGraphBuilder(optimizer, irMeta);
    }

    // FilterMatchRule, FieldTrimRule, TopKPushDownRule, ExpandGetVFusionRule, LateProjectionRule,
    // IndexScanRule
    @Test
    public void ic_2_test() throws Exception {
        // person.id, hascreator.creationDate, person.~id,
        List<String> params = Lists.newArrayList("933", "20120421224501754", "72057594037928869");
        FileUtils.writeStringToFile(
                logFile,
                "************************Run IC 2 Query************************\n\n\n",
                StandardCharsets.UTF_8,
                true);

        String indexed =
                "MATCH (p:PERSON)-[k:KNOWS]-(friend:PERSON)<-[h:HASCREATOR]-(message : POST |"
                        + " COMMENT) \n"
                        + "WHERE \n"
                        + "    elementId(p) = $3\n"
                        //                        + "    AND h.creationDate <= $2 \n"
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
                        //                        + "    AND h.creationDate <= $2 \n"
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
                        //                        + "    AND h.creationDate <= $2 \n"
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

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8, true);
    }

    // FilterIntoMatchRule, CommonPatternReuseRule, ExpandGetVFusionRule, DegreeFusionRule
    @Test
    public void bi_5_test() throws Exception {
        // tag.name
        List<String> params = Lists.newArrayList("\"North_German_Confederation\"");
        FileUtils.writeStringToFile(
                logFile,
                "************************Run BI 5 Query************************\n\n\n",
                StandardCharsets.UTF_8,
                true);
        String template =
                "Match (tag:TAG)<-[:HASTAG]-(message:POST|COMMENT)\n"
                        + "Where tag.name = $1\n"
                        + "WITH DISTINCT message\n"
                        + "MATCH (message)<-[:LIKES]-(liker:PERSON)\n"
                        + "WITH message, count(liker) as likeCount\n"
//                        + "MATCH (message)<-[:REPLYOF]-(comment:COMMENT)\n"
//                        + "WITH message, likeCount, count(comment) as replyCount\n"
//                        + "MATCH (message)-[:HASCREATOR]->(person:PERSON)\n"
//                        + "Return \n"
//                        + "  person.id AS id,\n"
//                        + "  sum(replyCount) as replyCount,\n"
//                        + "  sum(likeCount) as likeCount,\n"
//                        + "  count(message) as messageCount\n";
        + "Return count(message)\n";

        GraphPlanner.Summary all_plan =
                buildPhysical(
                        "all_plan",
                        template,
                        params,
                        false,
                        "FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule, DegreeFusionRule");
        System.out.println(all_plan.getLogicalPlan().explain());
//        System.out.println(all_plan.getPhysicalPlan().explain());
        executePhysical(all_plan);

        // remove DegreeFusionRule
        GraphPlanner.Summary no_degree_plan =
                buildPhysical(
                        "no_degree_plan",
                        template,
                        params,
                        false,
                        "FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule");
        System.out.println(no_degree_plan.getLogicalPlan().explain());
//        System.out.println(no_degree_plan.getPhysicalPlan().explain());
        executePhysical(no_degree_plan);

        // remove ExpandGetVFusionRule
//        GraphPlanner.Summary no_fusion_plan =
//                buildPhysical(
//                        "no_fusion_plan",
//                        template,
//                        params,
//                        false,
//                        "FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule");
//        executePhysical(no_fusion_plan);

//        // remove FlatJoinToExpandRule
//        GraphPlanner.Summary no_flat_plan =
//                buildPhysical(
//                        "no_flat_plan",
//                        template,
//                        params,
//                        false,
//                        "FilterMatchRule, ExtendIntersectRule");
//        executePhysical(no_flat_plan);

//        String join_twice =
//                "Match (tag:TAG)<-[:HASTAG]-(message:POST|COMMENT)<-[:LIKES]-(liker:PERSON)\n"
//                        + "Where tag.name = $1\n"
//                        + "WITH DISTINCT message\n"
//                        + "WITH message, count(liker) as likeCount\n"
//                        + "OPTIONAL MATCH (message)<-[:REPLYOF]-(comment:COMMENT)\n"
//                        + "WITH message, likeCount, count(comment) as replyCount\n"
//                        + "MATCH (message)-[:HASCREATOR]->(person:PERSON)\n"
//                        + "Return \n"
//                        + "  person.id AS id,\n"
//                        + "  sum(replyCount) as replyCount,\n"
//                        + "  sum(likeCount) as likeCount,\n"
//                        + "  count(message) as messageCount\n";
//        GraphPlanner.Summary join_twice_plan =
//                buildPhysical(
//                        "join_twice_plan",
//                        join_twice,
//                        params,
//                        false,
//                        "FilterMatchRule, ExtendIntersectRule");
//        executePhysical(join_twice_plan);
//
//        String join_once =
//                "Match (tag:TAG)<-[:HASTAG]-(message:POST|COMMENT)<-[:LIKES]-(liker:PERSON), (message)<-[:REPLYOF]-(comment:COMMENT)\n"
//                        + "Where tag.name = $1\n"
//                        + "WITH DISTINCT message\n"
//                        + "WITH message, count(liker) as likeCount\n"
//                        + "WITH message, likeCount, count(comment) as replyCount\n"
//                        + "MATCH (message)-[:HASCREATOR]->(person:PERSON)\n"
//                        + "Return \n"
//                        + "  person.id AS id,\n"
//                        + "  sum(replyCount) as replyCount,\n"
//                        + "  sum(likeCount) as likeCount,\n"
//                        + "  count(message) as messageCount\n";
//        GraphPlanner.Summary join_once_plan =
//                buildPhysical(
//                        "join_once_plan",
//                        join_once,
//                        params,
//                        false,
//                        "FilterMatchRule, ExtendIntersectRule");
//        executePhysical(join_once_plan);

//        // remove FilterIntoMatchRule
//        GraphPlanner.Summary no_filter_plan =
//                buildPhysical("no_filter_plan", template, params, false, "ExtendIntersectRule");
//        executePhysical(no_filter_plan);

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8, true);
    }

//    // ManyToOneIndexRule
//    @Test
//    public void advanced_index_test() {}

    @Test
    public void path_join_test() throws Exception {
        // person.id, tagclass.name
        List<String> params = Lists.newArrayList("933", "\"Organisation\"");
        FileUtils.writeStringToFile(
                logFile,
                "************************Run Path Join Query************************\n\n\n",
                StandardCharsets.UTF_8,
                true);
        String rules = "FilterMatchRule, FieldTrimRule, ExtendIntersectRule, ExpandGetVFusionRule";
        String gopt =
                "MATCH \n"
                    + "    (unused:PERSON {id:"
                    + " $1})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comments:COMMENT)-[:REPLYOF]->(:POST)-[:HASTAG]->(tags:TAG)\n"
                    + "WITH friend, comments, tags\n"
                    + "MATCH (tags:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF*0..10]->(:TAGCLASS"
                    + " {name: $2})\n"
                    + "WITH \n"
                    + "    friend AS friend, \n"
                    + "    collect(DISTINCT tags.name) AS tagNames, \n"
                    + "    count(DISTINCT comments) AS replyCount \n"
                    + "ORDER BY \n"
                    + "    replyCount DESC, \n"
                    + "    friend.id ASC \n"
                    + "LIMIT 20 \n"
                    + "RETURN \n"
                    + "    friend.id AS personId, \n"
                    + "    friend.firstName AS personFirstName, \n"
                    + "    friend.lastName AS personLastName, \n"
                    + "    tagNames, \n"
                    + "    replyCount";
        GraphPlanner.Summary goptPlan = buildPhysical("gopt_plan", gopt, params, false, rules);
        executePhysical(goptPlan);

        String alter1 =
                "MATCH \n"
                    + "    (unused:PERSON {id:"
                    + " $1})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comments:COMMENT)-[:REPLYOF]->(p1:POST)-[:HASTAG]->(tags:TAG)-[:HASTYPE]->(t1:TAGCLASS)\n"
                    + "WITH friend, comments, t1, tags\n"
                    + "MATCH (t1:TAGCLASS)-[:ISSUBCLASSOF*0..10]->(:TAGCLASS {name: $2})\n"
                    + "WITH \n"
                    + "    friend AS friend, \n"
                    + "    collect(DISTINCT tags.name) AS tagNames, \n"
                    + "    count(DISTINCT comments) AS replyCount \n"
                    + "ORDER BY \n"
                    + "    replyCount DESC, \n"
                    + "    friend.id ASC \n"
                    + "LIMIT 20 \n"
                    + "RETURN \n"
                    + "    friend.id AS personId, \n"
                    + "    friend.firstName AS personFirstName, \n"
                    + "    friend.lastName AS personLastName, \n"
                    + "    tagNames, \n"
                    + "    replyCount";
        GraphPlanner.Summary alter1Plan =
                buildPhysical("alter1_plan", alter1, params, false, rules);
        executePhysical(alter1Plan);

        String alter2 =
                "MATCH \n"
                    + "    (unused:PERSON {id:"
                    + " $1})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comments:COMMENT)-[:REPLYOF]->(p1:POST)\n"
                    + "WITH friend, comments, p1\n"
                    + "MATCH"
                    + " (p1:POST)-[:HASTAG]->(tags:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF*0..10]->(:TAGCLASS"
                    + " {name: $2})\n"
                    + "WITH \n"
                    + "    friend AS friend, \n"
                    + "    collect(DISTINCT tags.name) AS tagNames, \n"
                    + "    count(DISTINCT comments) AS replyCount \n"
                    + "ORDER BY \n"
                    + "    replyCount DESC, \n"
                    + "    friend.id ASC \n"
                    + "LIMIT 20 \n"
                    + "RETURN \n"
                    + "    friend.id AS personId, \n"
                    + "    friend.firstName AS personFirstName, \n"
                    + "    friend.lastName AS personLastName, \n"
                    + "    tagNames, \n"
                    + "    replyCount";
        GraphPlanner.Summary alter2Plan =
                buildPhysical("alter2_plan", alter2, params, false, rules);
        executePhysical(alter2Plan);

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8, true);
    }

    // FilterIntoJoin, FilterIntoMatch, AggregatePushDown, PatternOptimization(MatchFusion,
    // JoinElimination), ExpandGetVFusion
    @Test
    public void cyclic_ablation_test() throws Exception {
        // person.id, hasmember.joinDate
        List<String> params = Lists.newArrayList("933", "20110428001507382");
        FileUtils.writeStringToFile(
                logFile,
                "************************Run Cyclic Ablation Query************************\n\n\n",
                StandardCharsets.UTF_8,
                true);
        String all =
                "MATCH (person:PERSON)-[:KNOWS]-(otherP),\n"
                        + "      (otherP)<-[membership:HASMEMBER]-(forum)\n"
                        + "\n"
                        + "MATCH (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
                        + "\n"
                        + "WHERE person.id = $1\n"
                        + "      AND otherP.id <> $1 \n"
                        + "      AND membership.joinDate > $2\n"
                        + "\n"
                        + "WITH otherP, count(distinct post) as post_cnt\n"
                        + "\n"
                        + "MATCH (otherP)<-[:HASCREATOR]-(:POST)-[:HASTAG]->(tag:TAG)\n"
                        + "\n"
                        + "Return otherP, sum(post_cnt) as post_cnt;";
        GraphPlanner.Summary allPlan =
                buildPhysical(
                        "all_plan",
                        all,
                        params,
                        false,
                        "FilterIntoJoinRule, FilterMatchRule, FlatJoinToExpandRule,"
                                + " ExtendIntersectRule, ExpandGetVFusionRule");
        executePhysical(allPlan);

        // remove ExpandGetVFusion
        GraphPlanner.Summary no_fusion_plan =
                buildPhysical(
                        "no_fusion_plan",
                        all,
                        params,
                        false,
                        "FilterIntoJoinRule, FilterMatchRule, FlatJoinToExpandRule,"
                                + " ExtendIntersectRule");
        executePhysical(no_fusion_plan);

        // remove AggregatePushDown
        String no_aggregate =
                "MATCH (person:PERSON)-[:KNOWS]-(otherP),\n" +
                        "      (otherP)<-[membership:HASMEMBER]-(forum)\n" +
                        "MATCH (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n" +
                        "\n" +
                        "WHERE person.id = $1\n" +
                        "      AND otherP.id <> $1 \n" +
                        "      AND membership.joinDate > $2\n" +
                        "\n" +
                        "MATCH (otherP)<-[:HASCREATOR]-(:POST)-[:HASTAG]->(tag:TAG)\n" +
                        "Return otherP, count(distinct post) as post_cnt;";
        GraphPlanner.Summary no_aggregate_plan =
                buildPhysical(
                        "no_aggregate_plan",
                        no_aggregate,
                        params,
                        false,
                        "FilterIntoJoinRule, FilterMatchRule, FlatJoinToExpandRule, ExtendIntersectRule");
        executePhysical(no_aggregate_plan);

        // remove CommonPatternReuse
        GraphPlanner.Summary no_reuse_plan =
                buildPhysical(
                        "no_reuse_plan",
                        all,
                        params,
                        false,
                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule");
        executePhysical(no_reuse_plan);

        // remove MatchFusionRule
        String no_match_fusion =
                "MATCH (person:PERSON)-[:KNOWS]-(otherP),\n"
                        + "      (otherP)<-[membership:HASMEMBER]-(forum)\n"
                        + "\n"
                        + "\n"
                        + "WHERE person.id = $1\n"
                        + "      AND otherP.id <> $1 \n"
                        + "      AND membership.joinDate > $2\n"
                        + "\n"
                        + "WITH otherP\n"
                        + "\n"
                        + "MATCH (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
                        + "\n"
                        + "\n"
                        + "WITH otherP, count(distinct post) as post_cnt\n"
                        + "\n"
                        + "MATCH (otherP)<-[:HASCREATOR]-(:POST)-[:HASTAG]->(tag:TAG)\n"
                        + "\n"
                        + "Return otherP, sum(post_cnt) as post_cnt;";
        GraphPlanner.Summary no_match_fusion_plan =
                buildPhysical(
                        "no_match_fusion_plan",
                        no_match_fusion,
                        params,
                        false,
                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule");
        executePhysical(no_match_fusion_plan);

        // remove FilterMatch
        GraphPlanner.Summary no_match_filter_plan =
                buildPhysical(
                        "no_match_filter_plan",
                        no_aggregate,
                        params,
                        false,
                        "FilterIntoJoinRule, ExtendIntersectRule");
        executePhysical(no_match_filter_plan);

        // remove FilterIntoJoin
        String no_join_filter =
                "MATCH (person:PERSON)-[:KNOWS]-(otherP),\n"
                        + "      (otherP)<-[membership:HASMEMBER]-(forum)\n"
                        + "\n"
                        + "WITH otherP, person, membership\n"
                        + "\n"
                        + "MATCH (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
                        + "\n"
                        + "MATCH (otherP)<-[:HASCREATOR]-(:POST)-[:HASTAG]->(tag:TAG)\n"
                        + "\n"
                        + "WHERE person.id = $1\n"
                        + "      AND otherP.id <> $1 \n"
                        + "      AND membership.joinDate > $2\n"
                        + "\n"
                        + "Return otherP, count(distinct post) as post_cnt;";
        GraphPlanner.Summary no_join_filter_plan =
                buildPhysical(
                        "no_join_filter_plan",
                        no_join_filter,
                        params,
                        false,
                        "ExtendIntersectRule");
        executePhysical(no_join_filter_plan);

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8, true);
    }

//    @Test
//    public void cyclic_pattern_test() throws Exception {
//        // person.id, hasmember.joinDate
//        List<String> params = Lists.newArrayList("933", "20110428001507382");
//        FileUtils.writeStringToFile(
//                logFile,
//                "************************Run Cyclic Pattern Query************************\n\n\n",
//                StandardCharsets.UTF_8,
//                true);
//        String gopt =
//                "MATCH (person:PERSON {id: $1})-[:KNOWS]-(otherP),\n"
//                        + "      (otherP)<-[membership:HASMEMBER]-(forum),\n"
//                        + "      (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
//                        + "WHERE otherP.id <> $1\n"
//                        + "      AND membership.joinDate > $2 Return count(person)";
//        GraphPlanner.Summary gopt_plan =
//                buildPhysical(
//                        "gopt_plan",
//                        gopt,
//                        params,
//                        false,
//                        "FilterMatchRule, ExtendIntersectRule,"
//                            + " ExpandGetVFusionRule");
//        executePhysical(gopt_plan);
//
//        String alter1 =
//                "MATCH (person:PERSON {id:"
//                    + " $1})-[:KNOWS]-(otherP)<-[membership:HASMEMBER]-(forum)\n"
//                    + "WHERE otherP.id <> $1\n"
//                    + "      AND membership.joinDate > $2\n"
//                    + "WITH forum, otherP\n"
//                    + "MATCH (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
//                    + "Return count(otherP);";
//        GraphPlanner.Summary alter1_plan =
//                buildPhysical(
//                        "alter1_plan",
//                        alter1,
//                        params,
//                        false,
//                        "FilterMatchRule, FlatJoinToIntersectRule,"
//                            + " ExtendIntersectRule, ExpandGetVFusionRule");
//        executePhysical(alter1_plan);
//
//        String alter2 =
//                "MATCH (person:PERSON {id: $1})-[:KNOWS]-(otherP),\n"
//                        + "      (otherP)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)\n"
//                        + "WHERE otherP.id <> $1\n"
//                        + "WITH otherP, forum\n"
//                        + "MATCH (otherP)<-[membership:HASMEMBER]-(forum)\n"
//                        + "WHERE membership.joinDate > $2\n"
//                        + "Return count(otherP);";
//
//        GraphPlanner.Summary alter2_plan =
//                buildPhysical(
//                        "alter2_plan",
//                        alter2,
//                        params,
//                        false,
//                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
//                            + " ExpandGetVFusionRule");
//        executePhysical(alter2_plan);
//
//        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8, true);
//    }

    @Test
    public void high_order_test() throws Exception {
        List<String> params = Lists.newArrayList("\"Mzumbe_morogoro\"");
        FileUtils.writeStringToFile(
                logFile,
                "************************Run High Order Query************************\n\n\n",
                StandardCharsets.UTF_8,
                true);
        // 3600s timeout
//        String low_order = "Match (c:PLACE {name: $1})<-[:ISLOCATEDIN]-(p1:PERSON),\n" +
//                "      (c)<-[:ISLOCATEDIN]-(p2:PERSON),\n" +
//                "      (p1)<-[:HASCREATOR]-(m1:COMMENT)<-[:LIKES]->(p2:PERSON)\n" +
//                "MATCH\n" +
//                "       (c:PLACE {name: $1})<-[:ISLOCATEDIN]-(p3:PERSON),\n" +
//                "       (c)<-[:ISLOCATEDIN]-(p4:PERSON),\n" +
//                "       (p3)-[:KNOWS|HASMODERATOR]-(m2:FORUM|PERSON)-[:KNOWS|HASMODERATOR]-(p4:PERSON)\n" +
//                "RETURN count(c)";
//
//        GraphPlanner.Summary low_order_plan =
//                buildPhysical(
//                        "low_order_plan",
//                        low_order,
//                        params,
//                        false,
//                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
//                                + " ExpandGetVFusionRule");
//        executePhysical(low_order_plan);

        String high_order = "Match (c:PLACE {name: $1})<-[:ISLOCATEDIN]-(p1:PERSON),\n" +
                "      (c)<-[:ISLOCATEDIN]-(p2:PERSON),\n" +
                "      (p1)<-[:HASCREATOR]-(m1:COMMENT)<-[:LIKES]->(p2:PERSON)\n" +
                "WITH c\n" +
                "MATCH\n" +
                "       (c:PLACE {name: $1})<-[:ISLOCATEDIN]-(p3:PERSON),\n" +
                "       (c)<-[:ISLOCATEDIN]-(p4:PERSON),\n" +
                "       (p3)-[:KNOWS|HASMODERATOR]-(m2:FORUM|PERSON)-[:KNOWS|HASMODERATOR]-(p4:PERSON)\n" +
                "RETURN count(c)";

        GraphPlanner.Summary high_order_plan =
                buildPhysical(
                        "high_order_plan",
                        high_order,
                        params,
                        false,
                        "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                + " ExpandGetVFusionRule");
        executePhysical(high_order_plan);

        FileUtils.writeStringToFile(logFile, "\n\n\n", StandardCharsets.UTF_8, true);
    }

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
                logFile, name + ": \n" + optimized.explain() + "\n", StandardCharsets.UTF_8, true);
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
                    logFile,
                    "execution time is ["
                            + elapsed
                            + "] ms, results is ["
                            + resultBuilder.substring(0, Math.min(resultBuilder.length(), 100))
                            + "]\n\n",
                    StandardCharsets.UTF_8,
                    true);
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile,
                    "execution error is " + e.getMessage().substring(0, Math.min(e.getMessage().length(), 100)) + "\n\n",
                    StandardCharsets.UTF_8,
                    true);
        }
    }
}
