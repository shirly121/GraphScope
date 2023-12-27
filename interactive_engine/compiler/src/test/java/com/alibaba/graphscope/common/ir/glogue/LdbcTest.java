package com.alibaba.graphscope.common.ir.glogue;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;

public class LdbcTest {
    @Test
    public void ldbc_5_test() {
        GraphRelOptimizer optimizer = createOptimizer();
        IrMeta ldbcMeta = Utils.mockSchemaMeta("schema/ldbc.json");
        GraphBuilder builder = createGraphBuilder(optimizer, ldbcMeta);
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON"
                                    + " {id:1})-[k:KNOWS]-(other)<-[hasMember:HASMEMBER]-(forum:FORUM),\n"
                                    + "    (other)<-[:HASCREATOR]-(post:POST)<-[:CONTAINEROF]-(forum)\n"
                                    + "WHERE hasMember.joinDate > 20100325\n"
                                    + "RETURN forum.title as title, forum.id as id, count(distinct"
                                    + " post) AS postCount\n"
                                    + "ORDER BY\n"
                                    + "    postCount DESC,\n"
                                    + "    id ASC\n"
                                    + "LIMIT 20",
                                builder)
                        .build();
        // System.out.println(node.explain());
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        // change the statistics of 'HASCREATOR'(set to 2909769) to change the plan order
    }

    @Test
    public void ldbc_7_test() {
        GraphRelOptimizer optimizer = createOptimizer();
        IrMeta ldbcMeta = Utils.mockSchemaMeta("schema/ldbc.json");
        GraphBuilder builder = createGraphBuilder(optimizer, ldbcMeta);
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " 1})<-[:HASCREATOR]-(message)<-[like:LIKES]-(liker:PERSON),\n"
                                    + "\t\t\t(liker)-[k:KNOWS]-(person)\n"
                                    + "WITH liker, message, like.creationDate AS likeTime, person\n"
                                    + "ORDER BY likeTime DESC, message.id ASC\n"
                                    + "WITH liker, person, head(collect(message)) as message,"
                                    + " head(collect(likeTime)) AS likeTime\n"
                                    + "RETURN\n"
                                    + "    liker.id AS personId,\n"
                                    + "    liker.firstName AS personFirstName,\n"
                                    + "    liker.lastName AS personLastName,\n"
                                    + "    likeTime AS likeCreationDate,\n"
                                    + "    message.id AS commentOrPostId,\n"
                                    + "    (likeTime - message.creationDate)/1000.0/60.0 AS"
                                    + " minutesLatency\n"
                                    + "ORDER BY\n"
                                    + "    likeCreationDate DESC,\n"
                                    + "    personId ASC\n"
                                    + "LIMIT 20",
                                builder)
                        .build();
        // System.out.println(node.explain());
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_8_test() {
        GraphRelOptimizer optimizer = createOptimizer();
        IrMeta ldbcMeta = Utils.mockSchemaMeta("schema/ldbc.json");
        GraphBuilder builder = createGraphBuilder(optimizer, ldbcMeta);
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (person:PERSON {id:"
                                    + " 1})<-[:HASCREATOR]-(message)<-[:REPLYOF]-(comment:COMMENT)-[:HASCREATOR]->(author:PERSON)\n"
                                    + "RETURN \n"
                                    + "\tauthor.id,\n"
                                    + "\tauthor.firstName,\n"
                                    + "\tauthor.lastName,\n"
                                    + "\tcomment.creationDate as commentDate,\n"
                                    + "\tcomment.id as commentId,\n"
                                    + "\tcomment.content\n"
                                    + "ORDER BY\n"
                                    + "\tcommentDate desc,\n"
                                    + "\tcommentId asc\n"
                                    + "LIMIT 20",
                                builder)
                        .build();
        // System.out.println(node.explain());
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    private GraphRelOptimizer createOptimizer() {
        PlannerConfig plannerConfig =
                PlannerConfig.create(
                        new Configs(
                                ImmutableMap.of(
                                        "graph.planner.is.on",
                                        "true",
                                        "graph.planner.opt",
                                        "CBO",
                                        "graph.planner.rules",
                                        "FilterMatchRule, ExtendIntersectRule",
                                        "graph.planner.cbo.glogue.schema",
                                        "./conf/ldbc1_statistics.txt")));
        return new GraphRelOptimizer(plannerConfig);
    }

    private GraphBuilder createGraphBuilder(GraphRelOptimizer optimizer, IrMeta irMeta) {
        RelOptCluster optCluster =
                GraphOptCluster.create(optimizer.getMatchPlanner(), Utils.rexBuilder);
        optCluster.setMetadataQuerySupplier(() -> optimizer.createMetaDataQuery());
        return (GraphBuilder)
                GraphPlanner.relBuilderFactory.create(
                        optCluster, new GraphOptSchema(optCluster, irMeta.getSchema()));
    }
}
