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

package com.alibaba.graphscope.cypher.antlr4;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.planner.rules.FilterMatchRule;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Test;

public class MatchTest {
    @Test
    public void match_1_test() {
        RelNode source = Utils.eval("Match (n) Return n").build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[n], opt=[VERTEX])",
                source.explain().trim());
    }

    @Test
    public void match_2_test() {
        RelNode source =
                Utils.eval("Match (n:person)-[x:knows]->(y:person) Return n, x, y").build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], x=[x], y=[y], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[y], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[n], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                source.explain().trim());
    }

    @Test
    public void match_3_test() {
        RelNode match = Utils.eval("Match (a)-[]->(b), (b)-[]->(c) Return a, b, c").build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], c=[c], isAppend=[false])\n"
                    + "  GraphLogicalMultiMatch(input=[null],"
                    + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=true,"
                    + " tables=[software, person]}], alias=[b], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[a], opt=[VERTEX])\n"
                    + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
                    + " alias=[DEFAULT], opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[b], opt=[VERTEX])\n"
                    + "]}])",
                match.explain().trim());
    }

    // for the sentence `(a:person)-[b:knows*1..3]-(c:person)`:
    // b is a path_expand operator, expand base should be `knows` type, getV base should be any
    // vertex types adjacent to knows (currently we have not implemented type inference based on
    // graph schema, so all vertex types are considered here)
    // c is a getV operator which should be `person` type
    @Test
    public void match_4_test() {
        RelNode match =
                Utils.eval(
                                "Match (a:person)-[b:knows*1..3 {weight:1.0}]->(c:person {name:"
                                        + " 'marko'}) Return a, b")
                        .build();
        Assert.assertEquals(
                "GraphLogicalProject(a=[a], b=[b], isAppend=[false])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], fusedFilter=[[=(DEFAULT.name, 'marko')]], opt=[END])\n"
                    + "  GraphLogicalPathExpand(expand=[GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[knows]}], alias=[DEFAULT], fusedFilter=[[=(DEFAULT.weight,"
                    + " 1.0E0)]], opt=[OUT])\n"
                    + "], getV=[GraphLogicalGetV(tableConfig=[{isAll=true, tables=[software,"
                    + " person]}], alias=[DEFAULT], opt=[END])\n"
                    + "], offset=[1], fetch=[2], path_opt=[ARBITRARY], result_opt=[END_V],"
                    + " alias=[b])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                match.explain().trim());
    }

    @Test
    public void match_5_test() {
        RelNode match = Utils.eval("Match (n:person {age: $age}) Return n").build();
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                        + "  GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                        + " alias=[n], fusedFilter=[[=(DEFAULT.age, ?0)]], opt=[VERTEX])",
                match.explain().trim());
    }

    @Test
    public void match_6_test() {
        RelNode project = Utils.eval("Match (a:person {id: 2l}) Return a").build();
        GraphLogicalSource source = (GraphLogicalSource) project.getInput(0);
        RexCall condition = (RexCall) source.getFilters().get(0);
        Assert.assertEquals(
                SqlTypeName.BIGINT, condition.getOperands().get(1).getType().getSqlTypeName());
    }

    // Match (a:person)-[x:knows]->(b:person), (b:person)-[:knows]-(c:person)
    // Optional Match (a:person)-[]->(c:person)
    // Return a
    @Test
    public void match_7_test() {
        RelNode multiMatch =
                Utils.eval(
                                "Match (n) Return min(n.id)")
                        .build();
        System.out.println(multiMatch.getRowType().getFieldList().get(0).getType().isNullable());
//        RelNode join = multiMatch.getInput(0);
//        System.out.println(join.getRowType());
//        Assert.assertEquals(
//                "GraphLogicalProject(a=[a], isAppend=[false])\n"
//                    + "  LogicalJoin(condition=[AND(=(a, a), =(c, c))], joinType=[left])\n"
//                    + "    GraphLogicalMultiMatch(input=[null],"
//                    + " sentences=[{s0=[GraphLogicalGetV(tableConfig=[{isAll=false,"
//                    + " tables=[person]}], alias=[b], opt=[END])\n"
//                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[x],"
//                    + " opt=[OUT])\n"
//                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
//                    + " alias=[a], opt=[VERTEX])\n"
//                    + "], s1=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
//                    + " alias=[c], opt=[OTHER])\n"
//                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}],"
//                    + " alias=[DEFAULT], opt=[BOTH])\n"
//                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
//                    + " alias=[b], opt=[VERTEX])\n"
//                    + "]}])\n"
//                    + "    GraphLogicalSingleMatch(input=[null],"
//                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
//                    + " alias=[c], opt=[END])\n"
//                    + "  GraphLogicalExpand(tableConfig=[{isAll=true, tables=[created, knows]}],"
//                    + " alias=[DEFAULT], opt=[OUT])\n"
//                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
//                    + " alias=[a], opt=[VERTEX])\n"
//                    + "], matchOpt=[OPTIONAL])",
//                multiMatch.explain().trim());
    }

    @Test
    public void match_8_test() throws Exception {
        RelNode multiMatch =
                Utils.eval("Match (n {id:2}) Optional Match (n)-[]->(b) Where b is null Return n, b").build();
//        Filter filter = (Filter) multiMatch.getInput(0);
//        System.out.println(filter.getCondition());
        RelOptPlanner planner = com.alibaba.graphscope.common.ir.Utils.mockPlanner((RelRule) CoreRules.FILTER_INTO_JOIN.config
                .withRelBuilderFactory(
                        (RelOptCluster cluster, @Nullable RelOptSchema schema) ->
                                GraphBuilder.create(
                                        null, (GraphOptCluster) cluster, schema)
        ).toRule(), FilterMatchRule.Config.DEFAULT.toRule());
        planner.setRoot(multiMatch);
        RelNode after = planner.findBestExp();
        System.out.println(after.explain());
        try (PhysicalBuilder<byte[]> ffiBuilder =
                     new FfiPhysicalBuilder(
                             getMockGraphConfig(), com.alibaba.graphscope.common.ir.Utils.schemaMeta, new LogicalPlan(after))) {
            ffiBuilder.build();
            System.out.println(ffiBuilder.explain());
//            Assert.assertEquals(
//                    FileUtils.readJsonFromResource("case_when.json"), ffiBuilder.explain());
        }
//        GraphBuilder builder = com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder();
//        RexNode expr = builder.source(new SourceConfig(GraphOpt.Source.VERTEX, new LabelConfig(false).addLabel("person")))
//                .call(GraphStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, builder.variable(null, "age"), builder.variable(null, "name"));
//        System.out.println(expr.getType());
    }

    private Configs getMockGraphConfig() {
        return new Configs(ImmutableMap.of("servers", "1", "workers", "1"));
    }
}
