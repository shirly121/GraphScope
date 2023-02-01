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

package com.alibaba.graphscope.common.calcite;

import com.alibaba.graphscope.common.calcite.tools.GraphBuilder;
import com.alibaba.graphscope.common.calcite.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.calcite.tools.config.LabelConfig;
import com.alibaba.graphscope.common.calcite.tools.config.SourceConfig;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;

public class FilterTest {
    // source([person]).filter("XXX") are fused
    @Test
    public void equal_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig().labels(new LabelConfig(false).addLabel("person"));
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.EQUALS,
                                builder.variable(null, "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(equal).build();
        Assert.assertEquals(
                "rel#0:LogicalSource.(tableConfig={isAll=false,"
                        + " tables=[person]},fusedFilter=[=(DEFAULT.age, 10)])",
                filter.toString());
    }

    @Test
    public void greater_1_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig().labels(new LabelConfig(false).addLabel("person"));
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.variable(null, "age"),
                                builder.literal(10));
        RelNode filter = builder.filter(equal).build();
        Assert.assertEquals(
                "rel#0:LogicalSource.(tableConfig={isAll=false,"
                        + " tables=[person]},fusedFilter=[>(DEFAULT.age, 10)])",
                filter.toString());
    }

    // 20 > 10 -> always returns true, ignore the condition
    @Test
    public void greater_2_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig().labels(new LabelConfig(false).addLabel("person"));
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.literal(20),
                                builder.literal(10));
        RelNode filter = builder.filter(equal).build();
        Assert.assertEquals(
                "rel#0:LogicalSource.(tableConfig={isAll=false, tables=[person]})",
                filter.toString());
    }

    /**
     * 10 > 20 -> always returns false, create {@link LogicalValues} which carries all data types of the node before the filter
     */
    @Test
    public void greater_3_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig().labels(new LabelConfig(false).addLabel("person"));
        RexNode equal =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.literal(10),
                                builder.literal(20));
        // the node before the filter
        RelNode previous = builder.peek();
        RelNode filter = builder.filter(equal).build();
        System.out.println(filter);
        Assert.assertEquals(filter.getClass(), LogicalValues.class);
        Assert.assertEquals(filter.getRowType(), previous.getRowType());
    }

    @Test
    public void and_test() {
        GraphBuilder builder = SourceTest.mockGraphBuilder();
        SourceConfig sourceConfig =
                new SourceConfig().labels(new LabelConfig(false).addLabel("person"));
        RexNode condition1 =
                builder.source(sourceConfig)
                        .call(
                                GraphStdOperatorTable.GREATER_THAN,
                                builder.variable(null, "age"),
                                builder.literal(20));
        RexNode condition2 =
                builder.call(
                        GraphStdOperatorTable.EQUALS,
                        builder.variable(null, "name"),
                        builder.literal("marko"));
        RelNode filter = builder.filter(condition1, condition2).build();
        Assert.assertEquals(
                "rel#0:LogicalSource.(tableConfig={isAll=false,"
                        + " tables=[person]},fusedFilter=[AND(>(DEFAULT.age, 20), =(DEFAULT.name,"
                        + " 'marko'))])",
                filter.toString());
    }
}
