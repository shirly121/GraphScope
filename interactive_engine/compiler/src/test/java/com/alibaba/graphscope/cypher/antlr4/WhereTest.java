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

public class WhereTest {
    private GraphBuilder eval(String query) {
        return WithTest.mockCypherVisitor().visitOC_Where(MatchTest.parser(query).oC_Where());
    }

    @Test
    public void order_test_1() {
        RelNode order =
                eval("Where a.name = \"marko\" and b.weight < 2.0 or c.age + 10 < a.age").build();
        Assert.assertEquals(
                "LogicalFilter(condition=[OR(AND(=(a.name, 'marko'), <(b.weight, 2.0E0)),"
                    + " <(+(c.age, 10), a.age))])\n"
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
