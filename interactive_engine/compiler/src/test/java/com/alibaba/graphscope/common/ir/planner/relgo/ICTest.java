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

package com.alibaba.graphscope.common.ir.planner.relgo;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ICTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.join.min.pattern.size", "3",
                                "graph.planner.intersect.max.pattern.size", "2",
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, ExtendIntersectRule, JoinDecompositionRule,"
                                        + " ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc_schema_exp_hierarchy.json",
                        "statistics/ldbc30_hierarchy_statistics.json",
                        optimizer);
    }

    @AfterClass
    public static void afterClass() {
        if (optimizer != null) {
            optimizer.close();
        }
    }

    @Test
    public void person_knows_person_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (p1:PERSON)-[k:KNOWS]->(p2:PERSON) Return count(p1)",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(
                        after, SqlExplainLevel.NON_COST_ATTRIBUTES)
                .trim());
    }

    @Test
    public void person_2_knows_person_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (p1:PERSON {firstName: 'XX'})-[:KNOWS]->(p2:PERSON)" +
                                        " MATCH (p2:PERSON)-[:KNOWS]->(p3:PERSON {firstName: 'XX'}) Return count(p1)",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        GraphRelProtoPhysicalBuilder builder1 = new GraphRelProtoPhysicalBuilder(configs, irMeta, new LogicalPlan(after));
        System.out.println(builder1.build().explain());
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(
                        after, SqlExplainLevel.NON_COST_ATTRIBUTES)
                .trim());
    }
}
