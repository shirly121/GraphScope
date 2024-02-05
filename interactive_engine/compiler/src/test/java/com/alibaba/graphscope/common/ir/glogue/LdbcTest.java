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
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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
                                "conf/ldbc30_statistics.txt",
                                "graph.planner.join.min.pattern.size",
                                "4"));
        optimizer = new GraphRelOptimizer(new PlannerConfig(configs));
        ldbcMeta = Utils.mockSchemaMeta("schema/ldbc.json");
        builder = createGraphBuilder(optimizer, ldbcMeta);
    }

    @Test
    public void ldbc_1_test() throws Exception {
        //        String template =
        //                FileUtils.readFileToString(
        //                        new File("queries/ldbc_templates/query_1"),
        // StandardCharsets.UTF_8);
        //        Map<String, Object> params = ImmutableMap.of("id", 2199023348145L, "fName",
        // "\"Mikhail\"");
        //        String query = StringSubstitutor.replace(template, params, "$_", "_");
        //        System.out.println("query:\n" + query + "\n\n");
        String query =
                "MATCH (p: PERSON{id: 1939})-[k:KNOWS*1..5]-(f: PERSON)-[:ISLOCATEDIN]->(city),\n"
                        + "              (f)-[:WORKAT]->(:COMPANY)-[:ISLOCATEDIN]->(:COUNTRY),\n"
                        + "              (f)-[:STUDYAT]->(:UNIVERSITY)-[:ISLOCATEDIN]->(:CITY)\n"
                        + "        RETURN count(p);";
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
        planner.dump(new PrintWriter(new FileOutputStream("ldbc_1.plans"), true));
    }

    @Test
    public void ldbc_2_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_2"), StandardCharsets.UTF_8);
        Map<String, Object> params = ImmutableMap.of("id", 4026, "date", 20130301000000000L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_3_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_3"), StandardCharsets.UTF_8);
        Map<String, Object> params =
                ImmutableMap.of(
                        "id",
                        2199023421984L,
                        "xName",
                        "\"Laos\"",
                        "yName",
                        "\"United_States\"",
                        "date1",
                        20000505013715278L,
                        "date2",
                        20300604130807720L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
        planner.dump(new PrintWriter(new FileOutputStream("ldbc_3.plans"), true));
    }

    @Test
    public void ldbc_4_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_4"), StandardCharsets.UTF_8);
        Map<String, Object> params =
                ImmutableMap.of(
                        "id", 2783,
                        "date1", 20100111014617581L,
                        "date2", 20130604130807720L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_5_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_5"), StandardCharsets.UTF_8);
        Map<String, Object> params = ImmutableMap.of("id", 4026, "date", 20100325000000000L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
        planner.dump(new PrintWriter(new FileOutputStream("ldbc_5.plans"), true));
    }

    @Test
    public void ldbc_6_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_6"), StandardCharsets.UTF_8);
        Map<String, Object> params =
                ImmutableMap.of("id", 4026, "tag", "\"North_German_Confederation\"");
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_7_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_7"), StandardCharsets.UTF_8);
        Map<String, Object> params = ImmutableMap.of("id", 4026);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_8_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_8"), StandardCharsets.UTF_8);
        Map<String, Object> params = ImmutableMap.of("id", 4026);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_9_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_9"), StandardCharsets.UTF_8);
        Map<String, Object> params = ImmutableMap.of("id", 4026, "date", 20130301000000000L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_11_test() throws Exception {
        //        String template =
        //                FileUtils.readFileToString(
        //                        new File("queries/ldbc_templates/query_11"),
        // StandardCharsets.UTF_8);
        //        Map<String, Object> params =
        //                ImmutableMap.of("id", "[1, 2, 3]", "name", "\"India\"", "year", 2012);
        //        String query = StringSubstitutor.replace(template, params, "$_", "_");
        String query =
                "MATCH (person:PERSON {id: [1, 2,"
                    + " 3]})-[:KNOWS*1..3]-(friend:PERSON)-[workAt:WORKAT]->(company)-[:ISLOCATEDIN]->(:PLACE"
                    + " {name: \"India\"})\n"
                    + "WHERE person <> friend\n"
                    + "Return count(person);";
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void ldbc_12_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_12"), StandardCharsets.UTF_8);
        Map<String, Object> params = ImmutableMap.of("id", 4026, "class", "\"Organisation\"");
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        System.out.println("query:\n" + query + "\n\n");
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void case_study_test_1() throws Exception {
        String query =
                "Match (p1:PERSON {id: [1, 2, 3]})-[:KNOWS*1..5]-(p2:PERSON {id: [1, 2, 3]}) Return"
                        + " count(p1)";
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
        planner.dump(new PrintWriter(System.out, true));
    }

    @Test
    public void case_study_test_2() throws Exception {
        String query =
                "Match (p1:person {id: 1})-[:p2p_social*4..5]->(p2:person {id: 2})\n"
                        + "Return count(p1);";
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
        planner.dump(new PrintWriter(new FileOutputStream("case_study.plans"), true));
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
