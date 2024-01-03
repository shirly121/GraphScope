//package com.alibaba.graphscope.common.ir.glogue;
//
//import com.alibaba.graphscope.common.ir.Utils;
//import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
//import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
//import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
//import com.alibaba.graphscope.common.store.IrMeta;
//import org.apache.calcite.plan.volcano.VolcanoPlanner;
//import org.apache.calcite.rel.RelNode;
//import org.junit.Test;
//
//import static com.alibaba.graphscope.common.ir.glogue.LdbcTest.createGraphBuilder;
//import static com.alibaba.graphscope.common.ir.glogue.LdbcTest.createOptimizer;
//
//public class RandomPickPlanTest {
//    @Test
//    public void random_pick_plan_test() {
//        GraphRelOptimizer optimizer = createOptimizer();
//        IrMeta ldbcMeta = Utils.mockSchemaMeta("schema/ldbc.json");
//        GraphBuilder builder = createGraphBuilder(optimizer, ldbcMeta);
//        RelNode node =
//                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
//                                "Match (person1:PERSON)-[:LIKES]->(message:COMMENT|POST), \n" +
//                                        "\t   (message:COMMENT|POST)-[:HASCREATOR]->(person2:PERSON), \n" +
//                                        "\t   (person1:PERSON)-[:ISLOCATEDIN]->(place:PLACE), \n" +
//                                        "     (person2:PERSON)-[:ISLOCATEDIN]->(place:PLACE)\n" +
//                                        "Return count(person1);",
//                                builder)
//                        .build();
//        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
//        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
//        planner.dump();
//        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
//    }
//}
