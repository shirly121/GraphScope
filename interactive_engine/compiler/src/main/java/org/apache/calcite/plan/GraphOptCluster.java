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

package org.apache.calcite.plan;

import com.alibaba.graphscope.common.calcite.planner.GraphHepPlanner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends from {@code RelOptCluster} to carry two more arguments:
 * one to generate alias id, another to generate {@code RelNode} id
 */
public class GraphOptCluster extends RelOptCluster {
    // to generate alias id increasingly in one query
    private final AtomicInteger nextAliasId;
    // to generate RelNode id increasingly in one query
    private final AtomicInteger nextRelNodeId;

    protected GraphOptCluster(
            RelOptPlanner planner,
            RelDataTypeFactory typeFactory,
            RexBuilder rexBuilder,
            AtomicInteger nextCorrel,
            Map<String, RelNode> mapCorrelToRel,
            AtomicInteger nextAliasId,
            AtomicInteger nextRelNodeId) {
        super(planner, typeFactory, rexBuilder, nextCorrel, mapCorrelToRel);
        this.nextAliasId = nextAliasId;
        this.nextRelNodeId = nextRelNodeId;
    }

    public static RelOptCluster create(RexBuilder rexBuilder) {
        return new GraphOptCluster(
                GraphHepPlanner.DEFAULT,
                rexBuilder.getTypeFactory(),
                rexBuilder,
                new AtomicInteger(0),
                new HashMap<>(),
                new AtomicInteger(0),
                new AtomicInteger(0));
    }

    public int getNextAliasId() {
        return nextAliasId.getAndIncrement();
    }

    public int getNextRelNodeId() {
        return nextRelNodeId.getAndIncrement();
    }
}
