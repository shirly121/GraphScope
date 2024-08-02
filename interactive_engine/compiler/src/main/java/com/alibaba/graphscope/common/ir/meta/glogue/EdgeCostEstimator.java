/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;

public abstract class EdgeCostEstimator<T> {
    protected final CountHandler handler;

    public EdgeCostEstimator(CountHandler handler) {
        this.handler = handler;
    }

    public abstract T estimate(Pattern srcPattern, PatternEdge edge, PatternVertex target);

    static class Join extends EdgeCostEstimator<ExpandJoin> {
        public Join(CountHandler handler) {
            super(handler);
        }

        @Override
        public ExpandJoin estimate(Pattern srcPattern, PatternEdge edge, PatternVertex target) {
            return null;
        }
    }

    static class Extend extends EdgeCostEstimator<Double> {
        public Extend(CountHandler handler) {
            super(handler);
        }

        @Override
        public Double estimate(Pattern srcPattern, PatternEdge edge, PatternVertex target) {
            PatternVertex src = Utils.getExtendFromVertex(edge, target);
            double targetSelectivity = target.getElementDetails().getSelectivity();
            if (Double.compare(targetSelectivity, 1.0d) != 0) {
                target =
                        (target instanceof SinglePatternVertex)
                                ? new SinglePatternVertex(
                                        target.getVertexTypeIds().get(0), target.getId())
                                : new FuzzyPatternVertex(target.getVertexTypeIds(), target.getId());
            }
            double edgeSelectivity = edge.getElementDetails().getSelectivity();
            if (Double.compare(targetSelectivity, 1.0d) != 0
                    || Double.compare(edgeSelectivity, 1.0d) != 0) {
                PatternVertex edgeSrc = (src == edge.getSrcVertex()) ? src : target;
                PatternVertex edgeDst = (src == edge.getSrcVertex()) ? target : src;
                edge =
                        edge instanceof SinglePatternEdge
                                ? new SinglePatternEdge(
                                        edgeSrc,
                                        edgeDst,
                                        edge.getEdgeTypeIds().get(0),
                                        edge.getId(),
                                        edge.isBoth(),
                                        edge.getElementDetails())
                                : new FuzzyPatternEdge(
                                        edgeSrc,
                                        edgeDst,
                                        edge.getEdgeTypeIds(),
                                        edge.getId(),
                                        edge.isBoth(),
                                        edge.getElementDetails());
            }
            Pattern pattern = new Pattern();
            pattern.addVertex(edge.getSrcVertex());
            pattern.addVertex(edge.getDstVertex());
            pattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
            return handler.handle(pattern) + handler.labelConstraintsDeltaCost(edge, target);
        }
    }
}