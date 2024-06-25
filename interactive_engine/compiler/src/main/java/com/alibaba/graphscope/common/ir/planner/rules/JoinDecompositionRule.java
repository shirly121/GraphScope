package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class JoinDecompositionRule<C extends JoinDecompositionRule.Config> extends RelRule<C> {

    protected JoinDecompositionRule(C config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        GraphPattern graphPattern = relOptRuleCall.rel(0);
        RelMetadataQuery mq = relOptRuleCall.getMetadataQuery();
        if (getMaxVertexNum(graphPattern.getPattern()) < config.getMinPatternSize()) {
            return;
        }
        graphPattern.setRowCount(mq.getRowCount(graphPattern));
        int queueCapacity = 1000;
        PriorityQueue<GraphJoinDecomposition> decompositionQueue =
                new PriorityQueue<>(queueCapacity, comparator.reversed()); // max heap
        (new JoinByEdge(graphPattern, mq, decompositionQueue, queueCapacity)).addDecompositions();
//        (new JoinByVertex(graphPattern, mq, decompositionQueue, queueCapacity)).addDecompositions();
        List<GraphJoinDecomposition> decompositionsInAscOrder = Lists.newArrayList();
        while (!decompositionQueue.isEmpty()) {
            GraphJoinDecomposition decomposition = decompositionQueue.poll();
            decompositionsInAscOrder.add(0, decomposition);
        }
        for (GraphJoinDecomposition decomposition : decompositionsInAscOrder) {
            relOptRuleCall.transformTo(decomposition);
        }
    }

    private class JoinByEdge {
        private final GraphPattern graphPattern;
        private final RelMetadataQuery mq;
        private final PriorityQueue<GraphJoinDecomposition> decompositionQueue;
        private final int queueCapacity;

        public JoinByEdge(
                GraphPattern graphPattern,
                RelMetadataQuery mq,
                PriorityQueue<GraphJoinDecomposition> decompositionQueue,
                int queueCapacity) {
            this.graphPattern = graphPattern;
            this.mq = mq;
            this.decompositionQueue = decompositionQueue;
            this.queueCapacity = queueCapacity;
        }

        public void addDecompositions() {
            Pattern pattern = graphPattern.getPattern();
            for (PatternEdge edge : pattern.getEdgeSet()) {
                PatternVertex srcVertex = edge.getSrcVertex();
                PatternVertex dstVertex = edge.getDstVertex();
                if (srcVertex != dstVertex
                        && pattern.getEdgesOf(srcVertex).size() > 1
                        && pattern.getEdgesOf(dstVertex).size() > 1) {
                    Pattern rightPattern = new Pattern(pattern);
                    rightPattern.removeEdge(edge);
                    if (rightPattern.isConnected()) {
                        Pattern leftPattern = new Pattern();
                        leftPattern.addVertex(srcVertex);
                        leftPattern.addVertex(dstVertex);
                        leftPattern.addEdge(srcVertex, dstVertex, edge);
                        leftPattern.reordering();
                        double leftCount =
                                mq.getRowCount(
                                        new GraphPattern(
                                                graphPattern.getCluster(),
                                                graphPattern.getTraitSet(),
                                                leftPattern));
                        double rightCount =
                                graphPattern.getRowCount()
                                        * getRowCount(graphPattern, srcVertex, mq)
                                        * getRowCount(graphPattern, dstVertex, mq)
                                        / leftCount;
                        GraphJoinDecomposition decomposition =
                                createJoinDecomposition(
                                        graphPattern,
                                        leftPattern,
                                        leftCount,
                                        rightPattern,
                                        rightCount,
                                        Lists.newArrayList(srcVertex, dstVertex));
                        decompositionQueue.add(decomposition);
//                        if (!containsDecomposition(decompositionQueue.iterator(), decomposition)) {
//                            if (decompositionQueue.size() < queueCapacity) {
//                                decompositionQueue.offer(decomposition);
//                            } else if (comparator.compare(decompositionQueue.peek(), decomposition)
//                                    > 0) {
//                                decompositionQueue.poll();
//                                decompositionQueue.offer(decomposition);
//                            }
//                        }
                    }
                }
            }
        }
    }

    private class JoinByVertex {
        private final GraphPattern graphPattern;
        private final RelMetadataQuery mq;
        private final PriorityQueue<GraphJoinDecomposition> decompositionQueue;
        private final int queueCapacity;

        public JoinByVertex(
                GraphPattern graphPattern,
                RelMetadataQuery mq,
                PriorityQueue<GraphJoinDecomposition> decompositionQueue,
                int queueCapacity) {
            this.graphPattern = graphPattern;
            this.mq = mq;
            this.decompositionQueue = decompositionQueue;
            this.queueCapacity = queueCapacity;
        }

        public void addDecompositions() {
            List<GraphJoinDecomposition> queues = initDecompositions();
            while (!queues.isEmpty()) {
                List<GraphJoinDecomposition> nextCompositions = getDecompositions(queues.remove(0));
                queues.addAll(nextCompositions);
            }
        }

        private List<GraphJoinDecomposition> initDecompositions() {
            Pattern pattern = graphPattern.getPattern();
            pattern.reordering();
            List<GraphJoinDecomposition> decompositions = Lists.newArrayList();
            for (PatternVertex vertex : pattern.getVertexSet()) {
                Pattern probePattern = new Pattern(vertex);
                probePattern.reordering();
                Map<Integer, Integer> probeOrderMap = Maps.newHashMap();
                probeOrderMap.put(
                        probePattern.getVertexOrder(vertex), pattern.getVertexOrder(vertex));
                Map<Integer, Integer> buildOrderMap = Maps.newHashMap();
                pattern.getVertexSet()
                        .forEach(
                                v -> {
                                    int orderId = pattern.getVertexOrder(v);
                                    buildOrderMap.put(orderId, orderId);
                                });
                GraphJoinDecomposition decomposition =
                        new GraphJoinDecomposition(
                                graphPattern.getCluster(),
                                graphPattern.getTraitSet(),
                                pattern,
                                probePattern,
                                pattern,
                                Lists.newArrayList(
                                        new GraphJoinDecomposition.JoinVertexPair(
                                                probePattern.getVertexOrder(vertex),
                                                pattern.getVertexOrder(vertex))),
                                new GraphJoinDecomposition.OrderMappings(
                                        probeOrderMap, buildOrderMap));
                double probeCount = getRowCount(graphPattern, vertex, mq);
                double buildCount = graphPattern.getRowCount();
                ((GraphPattern) decomposition.getLeft()).setRowCount(probeCount);
                ((GraphPattern) decomposition.getRight()).setRowCount(buildCount);
                decompositions.add(decomposition);
            }
            return decompositions;
        }

        private List<GraphJoinDecomposition> getDecompositions(GraphJoinDecomposition parent) {
            // try to put one edge from the build pattern into the probe pattern
            Pattern probePattern = parent.getProbePattern();
            Pattern buildPattern = parent.getBuildPattern();
            PatternVertex jointVertex =
                    buildPattern.getVertexByOrder(
                            parent.getJoinVertexPairs().get(0).getRightOrderId());
            double probeCount = ((GraphPattern) parent.getLeft()).getRowCount();
            double buildCount = ((GraphPattern) parent.getRight()).getRowCount();
            List<GraphJoinDecomposition> decompositions = Lists.newArrayList();
            Set<PatternEdge> edgeCandidates = buildPattern.getEdgesOf(jointVertex);
            for (PatternEdge edge : edgeCandidates) {
                PatternVertex disjointVertex = Utils.getExtendFromVertex(edge, jointVertex);
                Set<PatternEdge> disjointEdges = buildPattern.getEdgesOf(disjointVertex);
                PatternVertex newJointVertex = null;
                if (edgeCandidates.size() == 1 && disjointEdges.size() > 1) {
                    // disjoint become the new joint vertex
                    newJointVertex = disjointVertex;
                } else if (edgeCandidates.size() > 1 && disjointEdges.size() == 1) {
                    newJointVertex = jointVertex;
                }
                if (newJointVertex == null) {
                    continue;
                }
                Pattern probeClone = new Pattern(probePattern);
                if (!probeClone.containsVertex(edge.getSrcVertex())) {
                    probeClone.addVertex(edge.getSrcVertex());
                }
                if (!probeClone.containsVertex(edge.getDstVertex())) {
                    probeClone.addVertex(edge.getDstVertex());
                }
                probeClone.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
                probeClone.reordering();
                Pattern buildClone = new Pattern(buildPattern);
                buildClone.removeEdge(edge);
                if (!buildClone.isConnected()
                        || probeClone.getVertexNumber() > buildClone.getVertexNumber()) {
                    continue;
                }
                double probeCloneCount =
                        probeCount
                                * getRowCount(parent, edge, mq)
                                / getRowCount(parent, jointVertex, mq);
                double buildCloneCount =
                        buildCount
                                / getRowCount(parent, edge, mq)
                                * getRowCount(parent, newJointVertex, mq);
                GraphJoinDecomposition decomposition =
                        createJoinDecomposition(
                                new GraphPattern(
                                        parent.getCluster(),
                                        parent.getTraitSet(),
                                        parent.getParentPatten()),
                                probeClone,
                                probeCloneCount,
                                buildClone,
                                buildCloneCount,
                                Lists.newArrayList(newJointVertex));
                if (!containsDecomposition(decompositionQueue.iterator(), decomposition)) {
                    if (decompositionQueue.size() < queueCapacity) {
                        decompositionQueue.offer(decomposition);
                        decompositions.add(decomposition);
                    } else if (comparator.compare(decompositionQueue.peek(), decomposition) > 0) {
                        decompositionQueue.poll();
                        decompositionQueue.offer(decomposition);
                        decompositions.add(decomposition);
                    }
                }
            }
            return decompositions;
        }
    }

    private double getRowCount(RelNode parent, PatternEdge edge, RelMetadataQuery mq) {
        Pattern pattern = new Pattern();
        pattern.addVertex(edge.getSrcVertex());
        pattern.addVertex(edge.getDstVertex());
        pattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
        return mq.getRowCount(new GraphPattern(parent.getCluster(), parent.getTraitSet(), pattern));
    }

    private double getRowCount(RelNode parent, PatternVertex vertex, RelMetadataQuery mq) {
        Pattern pattern = new Pattern();
        pattern.addVertex(vertex);
        return mq.getRowCount(new GraphPattern(parent.getCluster(), parent.getTraitSet(), pattern));
    }

    private boolean containsDecomposition(
            Iterator<GraphJoinDecomposition> decompositions, GraphJoinDecomposition target) {
        Pattern targetProbe = ((GraphPattern) target.getLeft()).getPattern();
        Pattern targetBuild = ((GraphPattern) target.getRight()).getPattern();
        while (decompositions.hasNext()) {
            GraphJoinDecomposition d = decompositions.next();
            Pattern dProbe = ((GraphPattern) d.getLeft()).getPattern();
            Pattern dBuild = ((GraphPattern) d.getRight()).getPattern();
            if (dProbe.isIsomorphicTo(targetProbe) && dBuild.isIsomorphicTo(targetBuild)
                    || dProbe.isIsomorphicTo(targetBuild) && dBuild.isIsomorphicTo(targetProbe))
                return true;
        }
        return false;
    }

    private GraphJoinDecomposition createJoinDecomposition(
            GraphPattern graphPattern,
            Pattern probePattern,
            double probeCount,
            Pattern buildPattern,
            double buildCount,
            List<PatternVertex> jointVertices) {
        Pattern pattern = graphPattern.getPattern();
        List<GraphJoinDecomposition.JoinVertexPair> jointVertexPairs =
                jointVertices.stream()
                        .map(
                                k ->
                                        new GraphJoinDecomposition.JoinVertexPair(
                                                probePattern.getVertexOrder(k),
                                                buildPattern.getVertexOrder(k)))
                        .collect(Collectors.toList());
        Map<Integer, Integer> leftToTargetOrderMap = Maps.newHashMap();
        for (PatternVertex v1 : probePattern.getVertexSet()) {
            leftToTargetOrderMap.put(probePattern.getVertexOrder(v1), pattern.getVertexOrder(v1));
        }
        Map<Integer, Integer> rightToTargetOrderMap = Maps.newHashMap();
        for (PatternVertex v2 : buildPattern.getVertexSet()) {
            rightToTargetOrderMap.put(buildPattern.getVertexOrder(v2), pattern.getVertexOrder(v2));
        }
        GraphJoinDecomposition decomposition =
                new GraphJoinDecomposition(
                        graphPattern.getCluster(),
                        graphPattern.getTraitSet(),
                        pattern,
                        probePattern,
                        buildPattern,
                        jointVertexPairs,
                        new GraphJoinDecomposition.OrderMappings(
                                leftToTargetOrderMap, rightToTargetOrderMap));
        ((GraphPattern) decomposition.getLeft()).setRowCount(probeCount);
        ((GraphPattern) decomposition.getRight()).setRowCount(buildCount);
        return decomposition;
    }

    private int getMaxVertexNum(Pattern pattern) {
        int maxVertexNum = pattern.getVertexNumber();
        for (PatternEdge edge : pattern.getEdgeSet()) {
            if (edge.getElementDetails().getRange() != null) {
                PathExpandRange range = edge.getElementDetails().getRange();
                int maxHop = range.getOffset() + range.getFetch() - 1;
                maxVertexNum += (maxHop - 1);
            }
        }
        return maxVertexNum;
    }

    public static class Config implements RelRule.Config {
        public static JoinDecompositionRule.Config DEFAULT =
                new JoinDecompositionRule.Config()
                        .withOperandSupplier(b0 -> b0.operand(GraphPattern.class).anyInputs());

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;
        private int minPatternSize;

        @Override
        public RelRule toRule() {
            return new JoinDecompositionRule(this);
        }

        @Override
        public JoinDecompositionRule.Config withRelBuilderFactory(
                RelBuilderFactory relBuilderFactory) {
            this.builderFactory = relBuilderFactory;
            return this;
        }

        @Override
        public JoinDecompositionRule.Config withDescription(
                @org.checkerframework.checker.nullness.qual.Nullable String s) {
            this.description = s;
            return this;
        }

        @Override
        public JoinDecompositionRule.Config withOperandSupplier(OperandTransform operandTransform) {
            this.operandSupplier = operandTransform;
            return this;
        }

        public JoinDecompositionRule.Config withMinPatternSize(int minPatternSize) {
            this.minPatternSize = minPatternSize;
            return this;
        }

        @Override
        public OperandTransform operandSupplier() {
            return this.operandSupplier;
        }

        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable String description() {
            return this.description;
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            return this.builderFactory;
        }

        public int getMinPatternSize() {
            return minPatternSize;
        }
    }

    private static Comparator<GraphJoinDecomposition> comparator =
            (GraphJoinDecomposition d1, GraphJoinDecomposition d2) -> {
                double cost1 =
                        ((GraphPattern) d1.getLeft()).getRowCount()
                                + ((GraphPattern) d1.getRight()).getRowCount();
                double cost2 =
                        ((GraphPattern) d2.getLeft()).getRowCount()
                                + ((GraphPattern) d2.getRight()).getRowCount();
                return Double.compare(cost1, cost2);
            };
}
