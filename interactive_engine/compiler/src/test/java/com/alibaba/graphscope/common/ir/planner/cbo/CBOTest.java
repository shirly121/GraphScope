package com.alibaba.graphscope.common.ir.planner.cbo;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CBOTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.join.min.pattern.size",
                                "5",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule,"
                                        + " JoinDecompositionRule, ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc.json",
                        "statistics/ldbc30_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void Q1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (message:COMMENT|POST)-[:HASCREATOR]->(PERSON:PERSON), \n"
                                        + "      (message:COMMENT|POST)-[:HASTAG]->(tag:TAG), \n"
                                        + "      (PERSON:PERSON)-[:HASINTEREST]->(tag:TAG)\n"
                                        + "Return count(PERSON);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[PERSON], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(tag, tag)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[tag], startAlias=[message],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-697155798]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASINTEREST]}],"
                    + " alias=[tag], startAlias=[PERSON], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-697155798]])\n"
                    + "common#-697155798:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[message], startAlias=[PERSON], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[PERSON], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void Q2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (PERSON1:PERSON)-[:LIKES]->(message:COMMENT|POST), \n"
                                    + "\t   (message:COMMENT|POST)-[:HASCREATOR]->(PERSON2:PERSON),"
                                    + " \n"
                                    + "\t   (PERSON1:PERSON)<-[:HASMODERATOR]-(place:FORUM), \n"
                                    + "     (PERSON2:PERSON)<-[:HASMODERATOR]-(place:FORUM)\n"
                                    + "Return count(PERSON1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[PERSON1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(message, message)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[message], startAlias=[PERSON1], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-1649429364]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[message], startAlias=[PERSON2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#-1649429364]])\n"
                    + "common#-1649429364:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMODERATOR]}],"
                    + " alias=[PERSON1], startAlias=[place], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMODERATOR]}],"
                    + " alias=[place], startAlias=[PERSON2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[PERSON2], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void Q3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (PERSON1:PERSON)<-[:HASCREATOR]-(comment:COMMENT), \n"
                                        + "\t  (comment:COMMENT)-[:REPLYOF]->(POST:POST),\n"
                                        + "\t  (POST:POST)<-[:CONTAINEROF]-(forum:FORUM),\n"
                                        + "\t  (forum:FORUM)-[:HASMEMBER]->(PERSON2:PERSON)\n"
                                        + "Return count(PERSON1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[PERSON1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[PERSON2], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, COMMENT,"
                    + " PERSON)]], alias=[PERSON1], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT, POST)]],"
                    + " alias=[comment], startAlias=[POST], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[POST], startAlias=[forum], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void Q4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (forum:FORUM)-[:CONTAINEROF]->(POST:POST),\n"
                                        + "\t  (forum:FORUM)-[:HASMEMBER]->(PERSON1:PERSON), \n"
                                        + "\t  (forum:FORUM)-[:HASMEMBER]->(PERSON2:PERSON), \n"
                                        + "    (PERSON1:PERSON)-[:KNOWS]->(PERSON2:PERSON), \n"
                                        + "\t  (PERSON1:PERSON)-[:LIKES]->(POST:POST),\n"
                                        + "\t  (PERSON2:PERSON)-[:LIKES]->(POST:POST)\n"
                                        + "Return count(PERSON1);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[PERSON1], aggFunction=COUNT, alias='$f0',"
                    + " distinct=false}]])\n"
                    + "  MultiJoin(joinFilter=[=(PERSON1, PERSON1)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER, INNER]], outerJoinConditions=[[NULL, NULL,"
                    + " NULL]], projFields=[[ALL, ALL, ALL]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[PERSON1], startAlias=[POST], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#803682572]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[PERSON1], startAlias=[PERSON2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#803682572]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[PERSON1], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      CommonTableScan(table=[[common#803682572]])\n"
                    + "common#803682572:\n"
                    + "MultiJoin(joinFilter=[=(PERSON2, PERSON2)], isFullOuterJoin=[false],"
                    + " joinTypes=[[INNER, INNER]], outerJoinConditions=[[NULL, NULL]],"
                    + " projFields=[[ALL, ALL]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST)]],"
                    + " alias=[PERSON2], startAlias=[POST], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-1025398524]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASMEMBER]}],"
                    + " alias=[PERSON2], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    CommonTableScan(table=[[common#-1025398524]])\n"
                    + "common#-1025398524:\n"
                    + "GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[CONTAINEROF]}],"
                    + " alias=[POST], startAlias=[forum], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void Q5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);

        // The optimized order is from 'b' to 'a', which is opposite to the user-given order.
        // Verify that the path expand type is correctly inferred in this situation.
        RelNode before1 =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a)-[*1..2]->(b:COMMENT) Return a", builder)
                        .build();
        RelNode after1 = optimizer.optimize(before1, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON, COMMENT]}],"
                    + " alias=[a], opt=[END])\n"
                    + "    GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES,"
                    + " PERSON, COMMENT), EdgeLabel(REPLYOF, COMMENT, COMMENT)]], alias=[_],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[1], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[b])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[b], opt=[VERTEX])",
                after1.explain().trim());

        // check the type of path expand if the order is from 'a' to 'b'
        RelNode before2 =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (a {id: 1})-[*1..2]->(b:COMMENT) Return a", builder)
                        .build();
        RelNode after2 = optimizer.optimize(before2, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], isAppend=[false])\n"
                    + "  GraphLogicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}], alias=[b],"
                    + " opt=[END])\n"
                    + "    GraphLogicalPathExpand(fused=[GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[COMMENT]}], alias=[_], opt=[END], physicalOpt=[ITSELF])\n"
                    + "  GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, COMMENT),"
                    + " EdgeLabel(REPLYOF, COMMENT, COMMENT)]], alias=[_], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[1], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[_], start_alias=[a])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON,"
                    + " COMMENT]}], alias=[a], fusedFilter=[[=(_.id, 1)]], opt=[VERTEX])",
                after2.explain().trim());
    }

    @Test
    public void Q6_test() {
        long startTime = System.currentTimeMillis();
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (forum:FORUM)-[:HASMEMBER]->(person:PERSON),\n"
                                        + "      (person:PERSON)-[:ISLOCATEDIN]->(city:PLACE),\n"
                                        + "      (city:PLACE)-[:ISPARTOF]->(country:PLACE),\n"
                                        + "      (forum:FORUM)-[:CONTAINEROF]->(post:POST),\n"
                                        + "      (post:POST)<-[:REPLYOF]-(comment:COMMENT),\n"
                                        + "      (comment:COMMENT)-[:HASTAG]->(tag:TAG),\n"
                                        + "      (tag:TAG)-[:HASTYPE]->(tagClass:TAGCLASS)\n"
                                        + "RETURN COUNT(forum);",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("gopt time is " + elapsedTime);
    }
}
