package com.alibaba.graphscope.common.ir.glogue;

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.cypher.antlr4.Utils;

import org.apache.calcite.rel.RelNode;
import org.junit.Test;

public class TypeInferenceTest {
    @Test
    public void test_1() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode node =
                Utils.eval(
                                "Match (p1)<-[:HASCREATOR]-()<-[:CONTAINEROF]-() Return count(p1)",
                                builder)
                        .build();
        System.out.println(node.explain());
    }

    @Test
    public void test_2() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode node =
                Utils.eval("Match (p)-[]->(:ORGANISATION)-[]->(:PLACE) Return count(p)", builder)
                        .build();
        System.out.println(node.explain());
    }

    @Test
    public void test_3() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode node =
                Utils.eval("Match (p)<-[:ISLOCATEDIN]-()<-[]-(:PERSON) Return count(p)", builder)
                        .build();
        System.out.println(node.explain());
    }

    @Test
    public void test_4() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode node =
                Utils.eval(
                                "Match (p1)<-[]-(p2:POST), (p1)<-[:HASMODERATOR]-()-[]->(p2) Return"
                                        + " count(p1)",
                                builder)
                        .build();
        System.out.println(node.explain());
    }

    @Test
    public void test_5() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode node =
                Utils.eval(
                                "Match (p1)<-[]-(p2:POST), (p1)<-[:HASMODERATOR]-()-[]->(p2),"
                                        + " (p1)-[]->(:ORGANISATION)-[]->(p3:PLACE),"
                                        + " (p3)<-[:ISLOCATEDIN]-()<-[]-(p1) Return count(p1)",
                                builder)
                        .build();
        System.out.println(node.explain());
    }

    @Test
    public void test_6() {
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder("schema/ldbc.json");
        RelNode node =
                Utils.eval(
                                "Match"
                                    + " (message:PERSON|FORUM)-[:KNOWS|HASMODERATOR]->(person:PERSON),"
                                    + " \n"
                                    + "      (message)-[]->(tag:TAG), \n"
                                    + "      (person)-[]->(tag)\n"
                                    + "Return count(person);",
                                builder)
                        .build();
        System.out.println(node.explain());
    }
}
