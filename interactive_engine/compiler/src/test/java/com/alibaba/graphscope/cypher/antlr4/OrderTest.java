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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class OrderTest {
    private GraphBuilder eval(String query) {
        return WithTest.mockCypherVisitor().visitOC_Order(MatchTest.parser(query).oC_Order());
    }

    @Test
    public void order_test_1() {
        RelNode order = eval("Order By a.name desc").build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[a.name], dir0=[DESC])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                order.explain().trim());
    }

    @Test
    public void order_test_2() {
        RelNode order = eval("Order By a.name desc, b.weight asc").build();
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[a.name], sort1=[b.weight], dir0=[DESC], dir1=[ASC])\n"
                    + "  GraphLogicalSingleMatch(input=[null],"
                    + " sentence=[GraphLogicalGetV(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[c], opt=[END])\n"
                    + "  GraphLogicalExpand(tableConfig=[{isAll=false, tables=[knows]}], alias=[b],"
                    + " opt=[OUT])\n"
                    + "    GraphLogicalSource(tableConfig=[{isAll=false, tables=[person]}],"
                    + " alias=[a], opt=[VERTEX])\n"
                    + "], matchOpt=[INNER])",
                order.explain().trim());
    }
}
