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
import org.junit.BeforeClass;
import org.junit.Test;

public class LdbcTest {
    private static Configs configs;
    private static GraphRelOptimizer optimizer;
    private static IrMeta ldbcMeta;
    private static GraphBuilder builder;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterMatchRule, ExtendIntersectRule,"
                                        + " JoinDecompositionRule, ExpandGetVFusionRule",
                                "graph.planner.cbo.glogue.schema",
                                "conf/ldbc30_hierarchy_statistics.txt"));
        optimizer = new GraphRelOptimizer(new PlannerConfig(configs));
        ldbcMeta = Utils.mockSchemaMeta("schema/ldbc_schema_exp_hierarchy.json");
        builder = createGraphBuilder(optimizer, ldbcMeta);
    }

    @Test
    public void ldbc_1_test() {
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p: PERSON{id: 30786325579101})-[:KNOWS*1..4]-(f:"
                                    + " PERSON)-[:ISLOCATEDIN]->(city),\n"
                                    + "    \t(f)-[:WORKAT]->(:COMPANY)-[:ISLOCATEDIN]->(:COUNTRY),\n"
                                    + "     "
                                    + " (f)-[:STUDYAT]->(:UNIVERSITY)-[:ISLOCATEDIN]->(:CITY)\n"
                                    + "WHERE f.firstName = \"Ian\" AND f.id <> 30786325579101\n"
                                    + "RETURN count(p);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_2_test() {
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (p:PERSON{id:"
                                    + " 19791209300143})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message"
                                    + " : POST | COMMENT) \n"
                                    + "WHERE message.creationDate < 20121128080000000 \n"
                                    + "return \n"
                                    + "\tfriend.id AS personId, \n"
                                    + "\tfriend.firstName AS personFirstName, \n"
                                    + "  friend.lastName AS personLastName, \n"
                                    + "  message.id AS postOrCommentId, \n"
                                    + "  message.content AS content,\n"
                                    + "  message.imageFile AS imageFile,\n"
                                    + "  message.creationDate AS postOrCommentCreationDate \n"
                                    + "ORDER BY \n"
                                    + "  postOrCommentCreationDate DESC, \n"
                                    + "  postOrCommentId ASC \n"
                                    + "LIMIT 20",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_3_test() {
        RelNode node =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                " MATCH (countryX:COUNTRY {name:"
                                    + " 'Laos'})<-[:ISLOCATEDIN]-(messageX)-[:HASCREATOR]->(otherP:PERSON),\n"
                                    + "       (countryY:COUNTRY {name:"
                                    + " 'United_States'})<-[:ISLOCATEDIN]-(messageY)-[:HASCREATOR]->(otherP:PERSON),\n"
                                    + "      "
                                    + " (otherP)-[:ISLOCATEDIN]->(city)-[:ISPARTOF]->(countryCity),\n"
                                    + "       (person:PERSON"
                                    + " {id:4026})-[:KNOWS]-()-[:KNOWS]-(otherP)\n"
                                    + "WHERE countryCity.name <> 'Laos' AND countryCity.name <>"
                                    + " 'United_States'\n"
                                    + "Return count(countryX);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_5_test() {
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
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_7_test() {
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
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_8_test() {
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
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    public static GraphBuilder createGraphBuilder(GraphRelOptimizer optimizer, IrMeta irMeta) {
        RelOptCluster optCluster =
                GraphOptCluster.create(optimizer.getMatchPlanner(), Utils.rexBuilder);
        optCluster.setMetadataQuerySupplier(() -> optimizer.createMetaDataQuery());
        return (GraphBuilder)
                GraphPlanner.relBuilderFactory.create(
                        optCluster, new GraphOptSchema(optCluster, irMeta.getSchema()));
    }
}
