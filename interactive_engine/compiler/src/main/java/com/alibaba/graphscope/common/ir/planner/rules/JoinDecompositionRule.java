package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.glogue.Utils;
import com.alibaba.graphscope.common.ir.meta.schema.foreign.ForeignKey;
import com.alibaba.graphscope.common.ir.meta.schema.foreign.ForeignKeyEntry;
import com.alibaba.graphscope.common.ir.meta.schema.foreign.ForeignKeyMeta;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.type.JoinVertexEntry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
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
        int queueCapacity = config.getJoinQueueCapacity();
        PriorityQueue<GraphJoinDecomposition> decompositionQueue =
                new PriorityQueue<>(queueCapacity, comparator.reversed()); // max heap
        if (getMaxEdgeNum(graphPattern.getPattern()) > 2) {
            (new JoinByVertex(graphPattern, mq, decompositionQueue, queueCapacity))
                    .addDecompositions();
        }
        if (config.getForeignKeyMeta() != null) {
            (new JoinByForeignKey(graphPattern, mq, decompositionQueue, queueCapacity))
                    .addDecompositions();
        }
        if (config.isJoinByEdgeEnabled()) {
            (new JoinByEdge(graphPattern, mq, decompositionQueue, queueCapacity))
                    .addDecompositions();
        }
        List<GraphJoinDecomposition> decompositionsInAscOrder = Lists.newArrayList();
        while (!decompositionQueue.isEmpty()) {
            GraphJoinDecomposition decomposition = decompositionQueue.poll();
            decompositionsInAscOrder.add(0, decomposition);
        }
        for (GraphJoinDecomposition decomposition : decompositionsInAscOrder) {
            relOptRuleCall.transformTo(decomposition);
        }
    }

    private class JoinByRule {
        protected final GraphPattern graphPattern;
        protected final RelMetadataQuery mq;
        protected final PriorityQueue<GraphJoinDecomposition> decompositionQueue;
        protected final int queueCapacity;

        public JoinByRule(
                GraphPattern graphPattern,
                RelMetadataQuery mq,
                PriorityQueue<GraphJoinDecomposition> decompositionQueue,
                int queueCapacity) {
            this.graphPattern = graphPattern;
            this.mq = mq;
            this.decompositionQueue = decompositionQueue;
            this.queueCapacity = queueCapacity;
        }

        public void addDecompositions() {}

        protected boolean addDecompositionToQueue(GraphJoinDecomposition decomposition) {
            if (!containsDecomposition(decompositionQueue.iterator(), decomposition)) {
                if (decompositionQueue.size() < queueCapacity) {
                    decompositionQueue.offer(decomposition);
                    return true;
                } else if (comparator.compare(decompositionQueue.peek(), decomposition) > 0) {
                    decompositionQueue.poll();
                    decompositionQueue.offer(decomposition);
                    return true;
                }
            }
            return false;
        }

        protected boolean containsDecomposition(
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

        protected double getRowCount(RelNode parent, PatternEdge edge, RelMetadataQuery mq) {
            Pattern pattern = new Pattern();
            pattern.addVertex(edge.getSrcVertex());
            pattern.addVertex(edge.getDstVertex());
            pattern.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
            return mq.getRowCount(
                    new GraphPattern(parent.getCluster(), parent.getTraitSet(), pattern));
        }

        protected double getRowCount(RelNode parent, PatternVertex vertex, RelMetadataQuery mq) {
            Pattern pattern = new Pattern();
            pattern.addVertex(vertex);
            return mq.getRowCount(
                    new GraphPattern(parent.getCluster(), parent.getTraitSet(), pattern));
        }

        protected List<Pattern> splitByEdge(Pattern pattern, PatternEdge edge) {
            Pattern clone = new Pattern(pattern);
            clone.removeEdge(edge, false);
            List<Set<PatternVertex>> connected = clone.getConnectedComponents();
            if (connected.size() != 2) return Lists.newArrayList();
            if (connected.get(0).contains(edge.getSrcVertex())) {
                return Lists.newArrayList(
                        createSubgraph(pattern, connected.get(0)),
                        createSubgraph(pattern, connected.get(1)));
            } else {
                return Lists.newArrayList(
                        createSubgraph(pattern, connected.get(1)),
                        createSubgraph(pattern, connected.get(0)));
            }
        }

        protected Pattern createSubgraph(Pattern pattern, Set<PatternVertex> components) {
            Pattern subgraph = new Pattern();
            for (PatternVertex vertex : components) {
                subgraph.addVertex(vertex);
                for (PatternEdge edge : pattern.getEdgesOf(vertex)) {
                    if (components.contains(edge.getSrcVertex())
                            && components.contains(edge.getDstVertex())) {
                        if (!subgraph.containsVertex(edge.getSrcVertex())) {
                            subgraph.addVertex(edge.getSrcVertex());
                        }
                        if (!subgraph.containsVertex(edge.getDstVertex())) {
                            subgraph.addVertex(edge.getDstVertex());
                        }
                        subgraph.addEdge(edge.getSrcVertex(), edge.getDstVertex(), edge);
                    }
                }
            }
            subgraph.reordering();
            return subgraph;
        }
    }

    private class JoinByForeignKey extends JoinByRule {
        public JoinByForeignKey(
                GraphPattern graphPattern,
                RelMetadataQuery mq,
                PriorityQueue<GraphJoinDecomposition> decompositionQueue,
                int queueCapacity) {
            super(graphPattern, mq, decompositionQueue, queueCapacity);
        }

        @Override
        public void addDecompositions() {
            Pattern pattern = graphPattern.getPattern();
            for (PatternEdge edge : pattern.getEdgeSet()) {
                if (!(edge instanceof SinglePatternEdge)) {
                    continue;
                }
                EdgeTypeId typeId = edge.getEdgeTypeIds().get(0);
                ForeignKeyEntry entry = config.getForeignKeyMeta().getForeignKeyEntry(typeId);
                if (entry == null) continue;
                List<Pattern> subgraph = splitByEdge(pattern, edge);
                if (subgraph.size() != 2) continue;
                Pattern srcPattern = subgraph.get(0);
                Pattern dstPattern = subgraph.get(1);
                if (srcPattern.getVertexNumber() > dstPattern.getVertexNumber()) {
                    return;
                }
                double srcCount =
                        mq.getRowCount(
                                new GraphPattern(
                                        graphPattern.getCluster(),
                                        graphPattern.getTraitSet(),
                                        srcPattern));
                double dstCount =
                        mq.getRowCount(
                                new GraphPattern(
                                        graphPattern.getCluster(),
                                        graphPattern.getTraitSet(),
                                        dstPattern));
                GraphJoinDecomposition decomposition =
                        createJoinDecomposition(
                                graphPattern,
                                srcPattern,
                                srcCount,
                                dstPattern,
                                dstCount,
                                Lists.newArrayList(
                                        getJoinVertex(edge.getSrcVertex(), edge, entry)));
                addDecompositionToQueue(decomposition);
            }
        }

        private JoinVertex getJoinVertex(
                PatternVertex probeVertex, PatternEdge edge, ForeignKeyEntry entry) {
            PatternVertex buildVertex = Utils.getExtendFromVertex(edge, probeVertex);
            String probeKeyName = null;
            String buildKeyName = null;
            for (ForeignKey key : entry) {
                if (probeVertex.getVertexTypeIds().contains(key.getLabelId())) {
                    probeKeyName = key.getKeyName();
                } else if (buildVertex.getVertexTypeIds().contains(key.getLabelId())) {
                    buildKeyName = key.getKeyName();
                }
            }
            Preconditions.checkArgument(
                    probeKeyName != null && buildKeyName != null,
                    "probe key name or build key name should not be null");
            return new JoinVertex(probeVertex, probeKeyName, buildVertex, buildKeyName, true);
        }
    }

    private class JoinByEdge extends JoinByRule {
        public JoinByEdge(
                GraphPattern graphPattern,
                RelMetadataQuery mq,
                PriorityQueue<GraphJoinDecomposition> decompositionQueue,
                int queueCapacity) {
            super(graphPattern, mq, decompositionQueue, queueCapacity);
        }

        @Override
        public void addDecompositions() {
            Pattern pattern = graphPattern.getPattern();
            for (PatternEdge edge : pattern.getEdgeSet()) {
                PatternVertex srcVertex = edge.getSrcVertex();
                PatternVertex dstVertex = edge.getDstVertex();
                if (srcVertex != dstVertex
                        && pattern.getEdgesOf(srcVertex).size() > 1
                        && pattern.getEdgesOf(dstVertex).size() > 1) {
                    Pattern rightPattern = new Pattern(pattern);
                    rightPattern.removeEdge(edge, true);
                    if (rightPattern.isConnected()) {
                        Pattern leftPattern = new Pattern();
                        leftPattern.addVertex(srcVertex);
                        leftPattern.addVertex(dstVertex);
                        leftPattern.addEdge(srcVertex, dstVertex, edge);
                        leftPattern.reordering();
                        if (leftPattern.getVertexNumber() > rightPattern.getVertexNumber()) {
                            return;
                        }
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
                                        Lists.newArrayList(
                                                new JoinVertex(srcVertex),
                                                new JoinVertex(dstVertex)));
                        addDecompositionToQueue(decomposition);
                    }
                }
            }
        }
    }

    private class JoinByVertex extends JoinByRule {
        public JoinByVertex(
                GraphPattern graphPattern,
                RelMetadataQuery mq,
                PriorityQueue<GraphJoinDecomposition> decompositionQueue,
                int queueCapacity) {
            super(graphPattern, mq, decompositionQueue, queueCapacity);
        }

        @Override
        public void addDecompositions() {
            List<GraphJoinDecomposition> queues = initDecompositions();
            while (!queues.isEmpty()) {
                List<GraphJoinDecomposition> nextCompositions = getDecompositions(queues.remove(0));
                queues.addAll(nextCompositions);
            }
            addPxdInnerVDecompositions();
        }

        private List<GraphJoinDecomposition> initDecompositions() {
            Pattern pattern = graphPattern.getPattern();
            double patternCount = graphPattern.getRowCount();
            List<GraphJoinDecomposition> decompositions = Lists.newArrayList();
            for (PatternVertex vertex : pattern.getVertexSet()) {
                Pattern probePattern = new Pattern(vertex);
                probePattern.reordering();
                double probeCount = getRowCount(graphPattern, vertex, mq);
                GraphJoinDecomposition decomposition =
                        createJoinDecomposition(
                                graphPattern,
                                probePattern,
                                probeCount,
                                pattern,
                                patternCount,
                                Lists.newArrayList(new JoinVertex(vertex)));
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
                buildClone.removeEdge(edge, true);
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
                                Lists.newArrayList(new JoinVertex(newJointVertex)));
                if (addDecompositionToQueue(decomposition)) {
                    decompositions.add(decomposition);
                }
            }
            return decompositions;
        }

        private void addPxdInnerVDecompositions() {
            Pattern pattern = graphPattern.getPattern();
            for (PatternEdge edge : pattern.getEdgeSet()) {
                if (edge.getElementDetails().getRange() == null) continue;
                int minHop = edge.getElementDetails().getRange().getOffset();
                int maxHop = minHop + edge.getElementDetails().getRange().getFetch() - 1;
                List<Pattern> subgraph = splitByEdge(pattern, edge);
                if (subgraph.size() != 2) continue;
                Pattern probePattern = subgraph.get(0);
                Pattern buildPattern = subgraph.get(1);
                if (probePattern.getVertexNumber() > buildPattern.getVertexNumber()) continue;
                PatternVertex src = edge.getSrcVertex();
                PatternVertex dst = edge.getDstVertex();
                if (maxHop >= config.getMinPatternSize() - 1 && maxHop == minHop) {
                    for (int i = 0; i <= minHop; ++i) {
                        for (int j = 1; j <= maxHop - 1; ++j) {
                            if (i <= j && (minHop - i) <= (maxHop - j)) {
                                // split the path expand into two path expands
                                // probe part: [i, j]
                                // build part: [minHop - i, maxHop - j]
                                // todo: re-infer the type of innerV, which should be inferred from
                                // union types of all possible intermediate getV
                                PatternVertex splitVertex = createSplitVertex(dst);
                                PatternEdge probeSplit =
                                        createSplitEdge(
                                                edge,
                                                src,
                                                splitVertex,
                                                new PathExpandRange(i, j - i + 1));
                                PatternEdge buildSplit =
                                        createSplitEdge(
                                                edge,
                                                splitVertex,
                                                dst,
                                                new PathExpandRange(
                                                        minHop - i, maxHop - j - (minHop - i) + 1));
                                Pattern probeClone = new Pattern(probePattern);
                                probeClone.addVertex(splitVertex);
                                probeClone.addEdge(src, splitVertex, probeSplit);
                                probeClone.reordering();
                                Pattern buildClone = new Pattern(buildPattern);
                                buildClone.addVertex(splitVertex);
                                buildClone.addEdge(splitVertex, dst, buildSplit);
                                buildClone.reordering();
                                double probeCount =
                                        mq.getRowCount(
                                                new GraphPattern(
                                                        graphPattern.getCluster(),
                                                        graphPattern.getTraitSet(),
                                                        probeClone));
                                double buildCount =
                                        mq.getRowCount(
                                                new GraphPattern(
                                                        graphPattern.getCluster(),
                                                        graphPattern.getTraitSet(),
                                                        buildClone));
                                // todo: maintain the order map for the new split vertex
                                GraphJoinDecomposition decomposition =
                                        createJoinDecomposition(
                                                graphPattern,
                                                probeClone,
                                                probeCount,
                                                buildClone,
                                                buildCount,
                                                Lists.newArrayList(new JoinVertex(splitVertex)));
                                addDecompositionToQueue(decomposition);
                            }
                        }
                    }
                }
            }
        }

        private PatternVertex createSplitVertex(PatternVertex oldVertex) {
            int randomId = UUID.randomUUID().hashCode();
            return (oldVertex instanceof SinglePatternVertex)
                    ? new SinglePatternVertex(
                            oldVertex.getVertexTypeIds().get(0), randomId, new ElementDetails())
                    : new FuzzyPatternVertex(
                            oldVertex.getVertexTypeIds(), randomId, new ElementDetails());
        }

        private PatternEdge createSplitEdge(
                PatternEdge oldEdge,
                PatternVertex newSrc,
                PatternVertex newDst,
                PathExpandRange newRange) {
            // Here, by setting the ID of the split edge to the same as the original edge,
            // the intention is to enable finding the details info of the previous edge.
            // This way, the final <GraphLogicalPathExpand> operator can include alias and filter
            // details.
            int newEdgeId = oldEdge.getId();
            ElementDetails newDetails =
                    new ElementDetails(
                            oldEdge.getElementDetails().getSelectivity(),
                            newRange,
                            oldEdge.getElementDetails().getPxdInnerGetVTypes(),
                            oldEdge.getElementDetails().getResultOpt(),
                            oldEdge.getElementDetails().getPathOpt());
            return (oldEdge instanceof SinglePatternEdge)
                    ? new SinglePatternEdge(
                            newSrc,
                            newDst,
                            oldEdge.getEdgeTypeIds().get(0),
                            newEdgeId,
                            oldEdge.isBoth(),
                            newDetails)
                    : new FuzzyPatternEdge(
                            newSrc,
                            newDst,
                            oldEdge.getEdgeTypeIds(),
                            newEdgeId,
                            oldEdge.isBoth(),
                            newDetails);
        }
    }

    private static class JoinVertex
            extends Pair<JoinVertexEntry<PatternVertex>, JoinVertexEntry<PatternVertex>> {
        private final boolean isForeignKey;

        public JoinVertex(PatternVertex singleVertex) {
            this(
                    new JoinVertexEntry(singleVertex, null),
                    new JoinVertexEntry(singleVertex, null),
                    false);
        }

        public JoinVertex(
                PatternVertex left,
                @Nullable String leftKeyName,
                PatternVertex right,
                @Nullable String rightKeyName,
                boolean isForeignKey) {
            this(
                    new JoinVertexEntry(left, leftKeyName),
                    new JoinVertexEntry(right, rightKeyName),
                    isForeignKey);
        }

        public JoinVertex(
                JoinVertexEntry<PatternVertex> left,
                JoinVertexEntry<PatternVertex> right,
                boolean isForeignKey) {
            super(left, right);
            this.isForeignKey = isForeignKey;
        }

        public GraphJoinDecomposition.JoinVertexPair convert(
                Pattern probePattern, Pattern buildPattern) {
            return new GraphJoinDecomposition.JoinVertexPair(
                    new JoinVertexEntry(
                            probePattern.getVertexOrder(left.getVertex()), left.getKeyName()),
                    new JoinVertexEntry(
                            buildPattern.getVertexOrder(right.getVertex()), right.getKeyName()),
                    isForeignKey);
        }

        public JoinVertex reverse() {
            return new JoinVertex(right, left, isForeignKey);
        }
    }

    private GraphJoinDecomposition createJoinDecomposition(
            GraphPattern graphPattern,
            Pattern probePattern,
            double probeCount,
            Pattern buildPattern,
            double buildCount,
            List<JoinVertex> jointVertices) {
        if (probeCount > buildCount) {
            return createJoinDecomposition(
                    graphPattern,
                    buildPattern,
                    buildCount,
                    probePattern,
                    probeCount,
                    jointVertices.stream().map(JoinVertex::reverse).collect(Collectors.toList()));
        }
        Pattern pattern = graphPattern.getPattern();
        List<GraphJoinDecomposition.JoinVertexPair> jointVertexPairs =
                jointVertices.stream()
                        .map(k -> k.convert(probePattern, buildPattern))
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

    private int getMaxEdgeNum(Pattern pattern) {
        int maxEdgeNum = pattern.getEdgeNumber();
        for (PatternEdge edge : pattern.getEdgeSet()) {
            if (edge.getElementDetails().getRange() != null) {
                PathExpandRange range = edge.getElementDetails().getRange();
                int maxHop = range.getOffset() + range.getFetch() - 1;
                maxEdgeNum += (maxHop - 1);
            }
        }
        return maxEdgeNum;
    }

    public static class Config implements RelRule.Config {
        public static JoinDecompositionRule.Config DEFAULT =
                new JoinDecompositionRule.Config()
                        .withOperandSupplier(b0 -> b0.operand(GraphPattern.class).anyInputs());

        private RelRule.OperandTransform operandSupplier;
        private @Nullable String description;
        private RelBuilderFactory builderFactory;
        private int minPatternSize;
        private boolean joinByEdgeEnabled;
        private @Nullable ForeignKeyMeta foreignKeyMeta;
        private int joinQueueCapacity;

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

        public JoinDecompositionRule.Config withJoinByEdgeEnabled(boolean joinByEdgeEnabled) {
            this.joinByEdgeEnabled = joinByEdgeEnabled;
            return this;
        }

        public JoinDecompositionRule.Config withForeignKeyMeta(ForeignKeyMeta foreignKeyMeta) {
            this.foreignKeyMeta = foreignKeyMeta;
            return this;
        }

        public JoinDecompositionRule.Config withJoinQueueCapacity(int joinQueueCapacity) {
            this.joinQueueCapacity = joinQueueCapacity;
            return this;
        }

        public int getMinPatternSize() {
            return minPatternSize;
        }

        public boolean isJoinByEdgeEnabled() {
            return joinByEdgeEnabled;
        }

        public @Nullable ForeignKeyMeta getForeignKeyMeta() {
            return foreignKeyMeta;
        }

        public int getJoinQueueCapacity() {
            return joinQueueCapacity;
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
