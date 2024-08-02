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

package org.apache.calcite.plan.volcano;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RandomOrderTest {
    private static Configs configs;
    private static ExecutionClient client;
    private static GraphRelOptimizer optimizer;
    private static IrMeta irMeta;
    private static File logFile;
    private static File queryDir;
    private static int limit;

    @BeforeClass
    public static void beforeClass() throws Exception {
        configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        queryDir = new File(System.getProperty("queries", "queries"));
        Preconditions.checkArgument(
                queryDir.exists() && queryDir.isDirectory(),
                queryDir + " is not a valid directory");
        logFile = new File(System.getProperty("log", "log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                new StaticIrMetaFetcher(new LocalIrMetaReader(configs), optimizer.getGlogueHolder())
                        .fetch()
                        .get();
        client = ExecutionClient.Factory.create(configs, new HostsRpcChannelFetcher(configs));
        limit = Integer.valueOf(System.getProperty("limit", "10"));
    }

    @Test
    public void run_test() throws Exception {
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            execute_one_query(
                    file.getName(),
                    FileUtils.readFileToString(file, StandardCharsets.UTF_8),
                    new Function<GraphIOProcessor, GraphShuttle>() {
                        @Override
                        public GraphShuttle apply(GraphIOProcessor ioProcessor) {
                            Random random = new SecureRandom();
                            RandomPickVisitor pickOptimizer =
                                    new RandomPickVisitor(
                                            ioProcessor,
                                            (VolcanoPlanner) optimizer.getMatchPlanner(),
                                            random,
                                            limit, file.getName());
                            return pickOptimizer;
                        }
                    });
            clear();
        }
    }

    private void clear() {
        VolcanoPlanner planner = (VolcanoPlanner) optimizer.getMatchPlanner();
        List<RelOptRule> rules = planner.getRules();
        planner.clear();
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
    }

    private void execute_one_query(
            String queryName, String query, Function<GraphIOProcessor, GraphShuttle> visitorFactory)
            throws Exception {
        FileUtils.writeStringToFile(
                logFile,
                String.format(
                        "********************************************************************************************\n"
                            + "%s: %s\n",
                        queryName, query),
                StandardCharsets.UTF_8,
                true);
        int timeout = FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs);
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        // apply filter push down optimize
        optimizer.getRelPlanner().setRoot(node);
        node = optimizer.getRelPlanner().findBestExp();
        // apply CBO optimize
        GraphIOProcessor ioProcessor = new GraphIOProcessor(builder, irMeta);
        RelNode results = node.accept(visitorFactory.apply(ioProcessor));
        if (results instanceof RelNodeList) {
            List<RelNode> rels = ((RelNodeList) results).rels;
            int i = 0;
            for (RelNode rel : rels) {
                try {
                    optimizer.getPhysicalPlanner().setRoot(rel);
                    LogicalPlan logicalPlan =
                            new LogicalPlan(optimizer.getPhysicalPlanner().findBestExp());
                    String logicalExplain =
                            com.alibaba.graphscope.common.ir.tools.Utils.toString(
                                    logicalPlan.getRegularQuery());
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format("logical plan %d: %s\n", i++, logicalExplain),
                            StandardCharsets.UTF_8,
                            true);
                    PhysicalPlan physicalPlan =
                            new GraphRelProtoPhysicalBuilder(configs, irMeta, logicalPlan).build();
                    int queryId = UUID.randomUUID().hashCode();
                    ExecutionRequest request =
                            new ExecutionRequest(
                                    BigInteger.valueOf(queryId),
                                    "ir_plan_" + queryId,
                                    logicalPlan,
                                    physicalPlan);
                    long startTime = System.currentTimeMillis();
                    StreamIterator<IrResult.Record> resultIterator = new StreamIterator<>();
                    client.submit(
                            request,
                            new ExecutionResponseListener() {
                                @Override
                                public void onNext(IrResult.Record record) {
                                    try {
                                        resultIterator.putData(record);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }

                                @Override
                                public void onCompleted() {
                                    try {
                                        resultIterator.finish();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    resultIterator.fail(t);
                                }
                            },
                            new QueryTimeoutConfig(timeout));
                    StringBuilder resultBuilder = new StringBuilder();
                    while (resultIterator.hasNext()) {
                        resultBuilder.append(resultIterator.next());
                        // resultIterator.next();
                    }
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format(
                                    "execution time %d ms, results: %s\n",
                                    elapsedTime,
                                    resultBuilder.substring(
                                            0, Math.min(10, resultBuilder.length()))),
                            StandardCharsets.UTF_8,
                            true);
                    String mode = System.getProperty("mode", "best");
                    if (mode.equals("best")) {
                        timeout = Math.min(timeout, (int) (elapsedTime * 2));
                    }
                } catch (Exception e) {
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format("execution exception %s\n", e.getMessage()),
                            StandardCharsets.UTF_8,
                            true);
                }
            }
        }
        FileUtils.writeStringToFile(logFile, String.format("\n\n\n"), StandardCharsets.UTF_8, true);
    }

    private class RelNodeList extends AbstractRelNode {
        private final List<RelNode> rels;

        public RelNodeList(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> rels) {
            super(cluster, traitSet);
            this.rels = rels;
        }
    }

    private class RandomPickVisitor extends GraphShuttle {
        private final GraphIOProcessor ioProcessor;
        private final VolcanoPlanner matchPlanner;
        private final Random random;
        private final int pickCount;

        private final String queryName;

        public RandomPickVisitor(
                GraphIOProcessor ioProcessor,
                VolcanoPlanner matchPlanner,
                Random random,
                int pickCount,
                String queryName) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
            this.random = random;
            this.pickCount = pickCount;
            this.queryName = queryName;
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return findBestWithRandomK(match);
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return findBestWithRandomK(match);
        }

        private RelNode findBestWithRandomK(AbstractLogicalMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            RelNode best = matchPlanner.findBestExp();
            List<RelNode> allRels = Lists.newArrayList();
            String mode = System.getProperty("mode", "best");
            switch (mode) {
                case "neo4j":
                    // add neo4j BI rules
                    switch (queryName) {
                        case "BI_2":
                            allRels.addAll(randomPickN(1, null, new Neo4j_BI_2()));
                            break;
                        case "BI_3":
                            allRels.addAll(randomPickN(1, null, new Neo4j_BI_3()));
                            break;
                        case "BI_5":
                            allRels.addAll(randomPickN(1, null, new Neo4j_BI_5()));
                            break;
                        case "BI_6":
                            allRels.addAll(randomPickN(1, null, new Neo4j_BI_6()));
                            break;
                        case "BI_9":
                            allRels.addAll(randomPickN(1, null, new Neo4j_BI_9()));
                            break;
                    }
                    break;
                case "best":
                    // add best
                    allRels.add(best);
                case "random":
                    // add random k
                    allRels.addAll(randomPickN(pickCount, best, new SourceHasFilter()));
                    break;
            }
            allRels =
                    allRels.stream()
                            .map(k -> ioProcessor.processOutput(k))
                            .collect(Collectors.toList());
            return new RelNodeList(match.getCluster(), match.getTraitSet(), allRels);
        }

        private List<RelNode> randomPickN(int count, @Nullable RelNode best, OrderRule rule) {
            Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
            ImmutableList<RelSet> allSets = ordering.immutableSortedCopy(matchPlanner.allSets);
            List<RelNode> randomRels = Lists.newArrayList();
            Set<String> randomDigests = Sets.newHashSet();
            if (best != null) {
                randomDigests.add(best.explain());
            }
            int maxIter = Math.max(500, count);
            RelSet rootSet = allSets.get(0);
            for (int i = 0; i < 1; ++i) {
                int times = 0;
                while (randomRels.size() < count && (times++ < maxIter)) {
                    RelNode randomRel = randomPickOne(matchPlanner, random, rootSet);
                    rule.go(randomRel);
                    String randomDigest = randomRel.explain();
                    if (rule.matched()
                            && !randomDigests.contains(randomDigest)) {
                        randomRels.add(randomRel);
                        randomDigests.add(randomDigest);
                    }
                    rule.reset();
                }
            }
            return randomRels;
        }

        private RelNode randomPickOne(VolcanoPlanner planner, Random random, RelSet root) {
            List<GraphPattern> patterns = Lists.newArrayList();
            List<RelNode> intersects = Lists.newArrayList();
            for (RelSubset subset : root.subsets) {
                for (RelNode rel : subset.getRelList()) {
                    if (rel instanceof GraphPattern) {
                        patterns.add((GraphPattern) rel);
                    } else if (rel instanceof GraphExtendIntersect
                            || rel instanceof GraphJoinDecomposition) {
                        if (rel.getInputs().stream()
                                .allMatch(k -> ((RelSubset) k).getBest() != null)) {
                            intersects.add(rel);
                        }
                    }
                }
            }
            RelNode rel =
                    intersects.isEmpty()
                            ? patterns.get(0)
                            : intersects.get(random.nextInt(intersects.size()));
            if (rel.getInputs().size() > 0) {
                List<RelNode> newInputs =
                        rel.getInputs().stream()
                                .map(k -> randomPickOne(planner, random, planner.getSet(k)))
                                .collect(Collectors.toList());
                rel = rel.copy(rel.getTraitSet(), newInputs);
            }
            return rel;
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            RelNode var6;
            RelNode child2 = child.accept(this);
            if (child2 instanceof RelNodeList) {
                List<RelNode> newRels =
                        ((RelNodeList) child2)
                                .rels.stream()
                                        .map(
                                                k -> {
                                                    if (k == child) {
                                                        return parent;
                                                    } else {
                                                        List<RelNode> newInputs =
                                                                new ArrayList(parent.getInputs());
                                                        newInputs.set(i, k);
                                                        return parent.copy(
                                                                parent.getTraitSet(), newInputs);
                                                    }
                                                })
                                        .collect(Collectors.toList());
                var6 = new RelNodeList(parent.getCluster(), parent.getTraitSet(), newRels);
            } else {
                if (child2 == child) {
                    RelNode var10 = parent;
                    return var10;
                }

                List<RelNode> newInputs = new ArrayList(parent.getInputs());
                newInputs.set(i, child2);
                var6 = parent.copy(parent.getTraitSet(), newInputs);
            }
            return var6;
        }
    }

    private abstract class OrderRule extends RelVisitor {
        public abstract boolean matched();

        public abstract void reset();
    }

    private class SourceHasFilter extends OrderRule {
        private boolean hasFilter = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex singleVertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    if (Double.compare(singleVertex.getElementDetails().getSelectivity(), 1.0d)
                            < 0) {
                        hasFilter = true;
                    }
                }
            }
        }

        @Override
        public boolean matched() {
            return hasFilter;
        }

        @Override
        public void reset() {
            hasFilter = false;
        }
    }

    private class BI_3_SourceTagClass extends OrderRule {
        private boolean tagClassAsSource = false;
        private boolean hasJoin = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex vertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    List<Integer> ids = vertex.getVertexTypeIds();
                    if (ids.size() == 1 && ids.get(0) == 6) {
                        tagClassAsSource = true;
                    }
                }
            } else if (node instanceof GraphJoinDecomposition) {
                hasJoin = true;
            }
        }

        @Override
        public boolean matched() {
            return tagClassAsSource;
        }

        @Override
        public void reset() {
            tagClassAsSource = false;
            hasJoin = false;
        }
    }

    private class Neo4j_BI_2 extends OrderRule {
        private boolean tagAsSource = false;
        private boolean hasJoin = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex vertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    List<Integer> ids = vertex.getVertexTypeIds();
                    if (ids.size() == 1 && ids.get(0) == 7) {
                        tagAsSource = true;
                    }
                }
            } else if (node instanceof GraphJoinDecomposition) {
                hasJoin = true;
            }
        }

        @Override
        public boolean matched() {
            return tagAsSource && !hasJoin;
        }

        @Override
        public void reset() {
            tagAsSource = false;
            hasJoin = false;
        }
    }

    private class Neo4j_BI_3 extends OrderRule {
        private boolean countryAsSource = false;
        private boolean hasJoin = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex vertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    List<Integer> ids = vertex.getVertexTypeIds();
                    if (ids.size() == 1 && ids.get(0) == 8 && Double.compare(vertex.getElementDetails().getSelectivity(), 1.0d) < 0) {
                        countryAsSource = true;
                    }
                }
            } else if (node instanceof GraphJoinDecomposition) {
                hasJoin = true;
            }
        }

        @Override
        public boolean matched() {
            return countryAsSource && !hasJoin;
        }

        @Override
        public void reset() {
            countryAsSource = false;
            hasJoin = false;
        }
    }

    private class Neo4j_BI_5 extends OrderRule {
        private boolean postAsSource = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex vertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    List<Integer> ids = vertex.getVertexTypeIds();
                    if (ids.size() == 1 && ids.get(0) == 3) {
                        postAsSource = true;
                    }
                }
            }
        }

        @Override
        public boolean matched() {
            return postAsSource;
        }

        @Override
        public void reset() {
            postAsSource = false;
        }
    }

    private class Neo4j_BI_6 extends OrderRule {
        private boolean postAsSource = false;
        private boolean personAsSource = false;
        private int joinCount = 0;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex vertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    List<Integer> ids = vertex.getVertexTypeIds();
                    if (ids.size() == 1 && ids.get(0) == 1) {
                        personAsSource = true;
                    } else if (ids.size() == 1 && ids.get(0) == 3) {
                        postAsSource = true;
                    }
                }
            } else if (node instanceof GraphJoinDecomposition) {
                ++joinCount;
            }
        }

        @Override
        public boolean matched() {
            return postAsSource && personAsSource && joinCount == 1;
        }

        @Override
        public void reset() {
            postAsSource = false;
            personAsSource = false;
            joinCount = 0;
        }
    }

    private class Neo4j_BI_9 extends OrderRule {
        private boolean msgAsSource = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof GraphPattern) {
                GraphPattern pattern = (GraphPattern) node;
                if (pattern.getPattern().getVertexNumber() == 1) {
                    PatternVertex vertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    List<Integer> ids = vertex.getVertexTypeIds();
                    if (ids.size() == 2 && ids.contains(3) && ids.contains(2)) {
                        msgAsSource = true;
                    }
                }
            }
        }

        @Override
        public boolean matched() {
            return msgAsSource;
        }

        @Override
        public void reset() {
            msgAsSource = false;
        }
    }
}
