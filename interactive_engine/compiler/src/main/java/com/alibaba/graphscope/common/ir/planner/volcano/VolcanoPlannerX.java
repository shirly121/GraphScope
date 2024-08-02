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

package com.alibaba.graphscope.common.ir.planner.volcano;

import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.gremlin.Utils;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class VolcanoPlannerX extends VolcanoPlanner {
    public int totalCount = 0;
    public int pruneCount = 0;

    @Override
    protected RelOptCost upperBoundForInputs(RelNode mExpr, RelOptCost upperBound) {
//        ++totalCount;
//        //        // todo: support pruning optimizations
//        RelSubset subset = getSubset(mExpr);
//        RelOptCost best = Utils.getFieldValue(RelSubset.class, subset, "bestCost");
//        if (best != null && !best.isInfinite()) {
//            upperBound = best;
//        }
//        if (!upperBound.isInfinite()) {
//            RelOptCost rootCost = mExpr.getCluster().getMetadataQuery().getNonCumulativeCost(mExpr);
//            if (rootCost != null && !rootCost.isInfinite()) {
//                upperBound = upperBound.minus(rootCost);
//            }
//            if (upperBound.isLt(costFactory.makeZeroCost())) {
//                ++pruneCount;
//                return upperBound;
//            }
//            if (!mExpr.getInputs().isEmpty()) {
//                double rowCount =
//                        mExpr.getCluster().getMetadataQuery().getRowCount(mExpr.getInput(0));
//                RelOptCost rowCost =
//                        mExpr.getCluster().getPlanner().getCostFactory().makeCost(rowCount, 0, 0);
//                RelOptCost copy = upperBound.minus(rowCost);
//                if (copy.isLt(costFactory.makeZeroCost())) {
//                    ++pruneCount;
//                    return copy;
//                }
//            }
//        }
        return upperBound;
    }

    @Override
    public @Nullable RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
        Preconditions.checkArgument(rel != null, "rel is null");
        if (rel instanceof RelSubset) {
            return Utils.getFieldValue(RelSubset.class, rel, "bestCost");
        }
        boolean noneConventionHasInfiniteCost =
                Utils.getFieldValue(VolcanoPlanner.class, this, "noneConventionHasInfiniteCost");
        if (noneConventionHasInfiniteCost
                && rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == Convention.NONE) {
            return costFactory.makeInfiniteCost();
        }
        RelOptCost cost = this.costFactory.makeZeroCost();
        for (RelNode input : rel.getInputs()) {
            RelOptCost inputCost = getCost(input, mq);
            if (inputCost == null || inputCost.isInfinite()) {
                return inputCost;
            }
            cost = cost.plus(inputCost);
        }
        RelOptCost relCost = mq.getNonCumulativeCost(rel);
        if (relCost == null) return null;
        return cost.plus(relCost);
    }

    @Override
    public boolean isLogical(RelNode rel) {
        return !(rel instanceof GraphExtendIntersect || rel instanceof GraphJoinDecomposition);
    }
}
