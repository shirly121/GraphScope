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
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
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
import com.alibaba.graphscope.common.ir.tools.GraphBuilderFactory;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.GraphBuilderVisitor;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CBOTest {

    public static void main(String[] args) throws Exception {
        Test test = new Test();
        String order = System.getProperty("order", "GOpt");
        switch (order) {
            case "GOpt":
                test.run_gopt();
                break;
            case "Random":
                test.run_random();
                break;
            default:
                test.run_neo4j();
        }
    }

    private static class Test {
        private final Configs configs;
        private final ExecutionClient client;
        private final GraphRelOptimizer optimizer;
        private final IrMeta ldbcMeta;
        private final File logFile;
        private final File queryDir;
        private final RexBuilder rexBuilder;
        public final RelBuilderFactory relBuilderFactory;

        public Test() throws Exception {
            configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
            queryDir =
                    new File(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("gopt/cbo")
                                    .toURI());
            Preconditions.checkArgument(
                    queryDir.exists() && queryDir.isDirectory(),
                    queryDir + " is not a valid directory");
            logFile = new File(System.getProperty("log", "log"));
            if (logFile.exists()) {
                logFile.delete();
            }
            logFile.createNewFile();
            optimizer = new GraphRelOptimizer(configs);
            ldbcMeta = new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
            client = ExecutionClient.Factory.create(configs, new HostsRpcChannelFetcher(configs));
            rexBuilder = new GraphRexBuilder(new GraphTypeFactoryImpl(configs));
            relBuilderFactory = new GraphBuilderFactory(configs);
        }

        public void run_gopt() throws Exception {
            List<File> files = Arrays.asList(queryDir.listFiles());
            Collections.sort(files, Comparator.comparing(File::getName));
            for (File file : files) {
                execute_one_query(
                        file.getName(),
                        FileUtils.readFileToString(file, StandardCharsets.UTF_8),
                        (GraphIOProcessor ioProcessor) -> {
                            Random random = new SecureRandom();
                            RandomPickOptimizer pickOptimizer =
                                    new RandomPickOptimizer(
                                            ioProcessor,
                                            (VolcanoPlanner) optimizer.getMatchPlanner(),
                                            random,
                                            0,
                                            true);
                            return pickOptimizer;
                        });
            }
        }

        public void run_random() throws Exception {
            List<File> files = Arrays.asList(queryDir.listFiles());
            Collections.sort(files, Comparator.comparing(File::getName));
            for (File file : files) {
                execute_one_query(
                        file.getName(),
                        FileUtils.readFileToString(file, StandardCharsets.UTF_8),
                        (GraphIOProcessor ioProcessor) -> {
                            Random random = new SecureRandom();
                            RandomPickOptimizer pickOptimizer =
                                    new RandomPickOptimizer(
                                            ioProcessor,
                                            (VolcanoPlanner) optimizer.getMatchPlanner(),
                                            random,
                                            10,
                                            false);
                            return pickOptimizer;
                        });
            }
        }

        public void run_neo4j() {}

        private void execute_one_query(
                String queryName,
                String query,
                Function<GraphIOProcessor, GraphShuttle> visitorFactory)
                throws Exception {
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************************["
                            + queryName
                            + "]*******************************************\n",
                    StandardCharsets.UTF_8,
                    true);
            GraphBuilder builder = mockGraphBuilder(optimizer, ldbcMeta);
            RelNode node = eval(query, builder).build();
            // apply filter push down optimize
            optimizer.getRelPlanner().setRoot(node);
            node = optimizer.getRelPlanner().findBestExp();
            // apply CBO optimize
            GraphIOProcessor ioProcessor = new GraphIOProcessor(builder, ldbcMeta);
            RelNode results = node.accept(visitorFactory.apply(ioProcessor));
            if (results instanceof RelNodeList) {
                List<RelNode> rels = ((RelNodeList) results).rels;
                int i = 0;
                for (RelNode rel : rels) {
                    try {
                        optimizer.getPhysicalPlanner().setRoot(rel);
                        LogicalPlan logicalPlan =
                                new LogicalPlan(optimizer.getPhysicalPlanner().findBestExp());
                        PhysicalPlan physicalPlan =
                                new GraphRelProtoPhysicalBuilder(configs, ldbcMeta, logicalPlan)
                                        .build();
                        ExecutionRequest request =
                                new ExecutionRequest(
                                        UUID.randomUUID().hashCode(),
                                        queryName,
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
                                new QueryTimeoutConfig(
                                        FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs)));
                        StringBuilder resultBuilder = new StringBuilder();
                        while (resultIterator.hasNext()) {
                            resultBuilder.append(resultIterator.next());
                            // resultIterator.next();
                        }
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        FileUtils.writeStringToFile(
                                logFile,
                                String.format("plan id [%d], latency [%d] ms\n", i++, elapsedTime),
                                StandardCharsets.UTF_8,
                                true);
                    } catch (Exception e) {
                        FileUtils.writeStringToFile(
                                logFile,
                                String.format("execution exception %s\n", e.getMessage()),
                                StandardCharsets.UTF_8,
                                true);
                    }
                }
            }
            FileUtils.writeStringToFile(
                    logFile, String.format("\n\n\n"), StandardCharsets.UTF_8, true);
        }

        private class RelNodeList extends AbstractRelNode {
            private final List<RelNode> rels;

            public RelNodeList(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> rels) {
                super(cluster, traitSet);
                this.rels = rels;
            }
        }

        private class RandomPickOptimizer extends GraphShuttle {
            private final GraphIOProcessor ioProcessor;
            private final VolcanoPlanner matchPlanner;
            private final Random random;
            private final int pickCount;
            private final boolean pickBest;

            public RandomPickOptimizer(
                    GraphIOProcessor ioProcessor,
                    VolcanoPlanner matchPlanner,
                    Random random,
                    int pickCount,
                    boolean pickBest) {
                this.ioProcessor = ioProcessor;
                this.matchPlanner = matchPlanner;
                this.matchPlanner.allSets.clear();
                this.random = random;
                this.pickCount = pickCount;
                this.pickBest = pickBest;
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
                if (pickBest) {
                    // add best
                    allRels.add(best);
                }
                // add random k
                allRels.addAll(randomPickN(pickCount, best));
                allRels =
                        allRels.stream()
                                .map(k -> ioProcessor.processOutput(k))
                                .collect(Collectors.toList());
                return new RelNodeList(match.getCluster(), match.getTraitSet(), allRels);
            }

            private List<RelNode> randomPickN(int count, RelNode best) {
                Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
                ImmutableList<RelSet> allSets = ordering.immutableSortedCopy(matchPlanner.allSets);
                List<RelNode> randomRels = Lists.newArrayList();
                Set<RelDigest> randomDigests = Sets.newHashSet();
                int maxIter = 100;
                RelSet rootSet = allSets.get(0);
                while (randomRels.size() < count && maxIter-- > 0) {
                    RelNode randomRel = randomPickOne(matchPlanner, random, rootSet);
                    SourceFilterVisitor visitor = new SourceFilterVisitor();
                    visitor.go(randomRel);
                    if (visitor.isSourceHasFilter()
                            && !randomDigests.contains(randomRel.getRelDigest())
                            && !best.getRelDigest().equals(randomRel.getRelDigest())) {
                        randomRels.add(randomRel);
                        randomDigests.add(randomRel.getRelDigest());
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
                                                                    new ArrayList(
                                                                            parent.getInputs());
                                                            newInputs.set(i, k);
                                                            return parent.copy(
                                                                    parent.getTraitSet(),
                                                                    newInputs);
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

        private class SourceFilterVisitor extends RelVisitor {
            private boolean sourceHasFilter = false;

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
                            sourceHasFilter = true;
                        }
                    }
                }
            }

            public boolean isSourceHasFilter() {
                return sourceHasFilter;
            }
        }

        private GraphBuilder mockGraphBuilder(GraphRelOptimizer optimizer, IrMeta irMeta) {
            RelOptCluster optCluster =
                    GraphOptCluster.create(optimizer.getMatchPlanner(), rexBuilder);
            optCluster.setMetadataQuerySupplier(() -> optimizer.createMetaDataQuery());
            return (GraphBuilder)
                    relBuilderFactory.create(
                            optCluster, new GraphOptSchema(optCluster, irMeta.getSchema()));
        }

        private GraphBuilder eval(String query, GraphBuilder builder) {
            return new GraphBuilderVisitor(builder).visit(new CypherAntlr4Parser().parse(query));
        }
    }
}
