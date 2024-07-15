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

package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ExtendWeightEstimator {
    private final CountHandler handler;

    public ExtendWeightEstimator(CountHandler handler) {
        this.handler = handler;
    }

    /**
     * estimate extend weight for each edge between {@code Pattern} src and {@code Pattern} dst
     * @param edge
     * @return
     */
    public double estimate(PatternEdge edge, PatternVertex target) {
        PatternVertex extendFrom = Utils.getExtendFromVertex(edge, target);
        Pattern vertexPattern = new Pattern(extendFrom);
        Pattern edgePattern = new Pattern();
        edgePattern.addVertex(edge.getSrcVertex());
        edgePattern.addVertex(edge.getDstVertex());
        edgePattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        return handler.handle(edgePattern) / handler.handle(vertexPattern);
    }

    /**
     * estimate total extend weight for all edges in an {@code ExtendStep}, and sort edges by their weight in a heuristic way
     * @param edges
     * @return
     */
    public double estimate(List<PatternEdge> edges, PatternVertex target) {
        // sort edges by their weight in ascending order
        Collections.sort(
                edges, Comparator.comparingDouble((PatternEdge edge) -> estimate(edge, target)));
        Pattern pattern = new Pattern();
        double totalWeight = 0.0d;
        List<PatternVertex> extendFromVertices = Lists.newArrayList();
        for (PatternEdge edge : edges) {
            pattern.addVertex(edge.getSrcVertex());
            pattern.addVertex(edge.getDstVertex());
            pattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
            extendFromVertices.add(Utils.getExtendFromVertex(edge, target));
            double weight = handler.handle(pattern);
            if (edge.getElementDetails().getRange() != null) {
                // add path expand intermediate count
                if (Double.compare(target.getElementDetails().getSelectivity(), 0.0d) != 0) {
                    weight += (weight / target.getElementDetails().getSelectivity());
                }
            }
            for (PatternVertex vertex : extendFromVertices) {
                weight /= handler.handle(new Pattern(vertex));
            }
            totalWeight += weight;
        }
        if (edges.size() == 1 && target.getElementDetails() != null) {
            double selectivity = target.getElementDetails().getSelectivity();
            if (Double.compare(selectivity, 1.0d) != 0) {
                double deltaWeight = estimate(getEdgeNoSelectivity(edges.get(0), target), target);
                totalWeight += deltaWeight * 1;
            }
        }
        return totalWeight;
    }

    private PatternEdge getEdgeNoSelectivity(PatternEdge edge, PatternVertex target) {
        if (target.getElementDetails() != null) {
            ElementDetails details = target.getElementDetails();
            if (Double.compare(details.getSelectivity(), 1.0d) != 0) {
                ElementDetails newDetails = new ElementDetails(1.0d, details.isOptional());
                PatternVertex newTarget =
                        target instanceof SinglePatternVertex
                                ? new SinglePatternVertex(
                                        target.getVertexTypeIds().get(0),
                                        target.getId(),
                                        newDetails)
                                : new FuzzyPatternVertex(
                                        target.getVertexTypeIds(), target.getId(), newDetails);
                PatternVertex edgeSrc =
                        edge.getSrcVertex() == target ? newTarget : edge.getSrcVertex();
                PatternVertex edgeDst =
                        edge.getDstVertex() == target ? newTarget : edge.getDstVertex();
                return edge instanceof SinglePatternEdge
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
        }
        return edge;
    }

    public interface CountHandler {
        double handle(Pattern pattern);
    }
}
