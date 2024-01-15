package org.apache.calcite.plan.volcano;

import static com.alibaba.graphscope.common.ir.glogue.LdbcTest.createGraphBuilder;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.GraphRelVisitor;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;

public class RandomPickPlanTest {
    private static Configs configs;
    private static ExecutionClient client;
    private static GraphRelOptimizer optimizer;
    private static IrMeta ldbcMeta;
    private static File logFile;
    private static File queryDir;

    @BeforeClass
    public static void beforeClass() throws Exception {
        configs = new Configs(System.getProperty("config"));
        queryDir = new File(System.getProperty("query"));
        Preconditions.checkArgument(
                queryDir.exists() && queryDir.isDirectory(),
                queryDir + " is not a valid directory");
        logFile = new File(System.getProperty("log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
        optimizer = new GraphRelOptimizer(new PlannerConfig(configs));
        ldbcMeta = new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
        client = ExecutionClient.Factory.create(configs, new HostsRpcChannelFetcher(configs));
    }

    @Test
    public void run_test() throws Exception {
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        int queryId = 0;
        for (File file : files) {
            execute_one_query(
                    queryId++,
                    file.getName(),
                    FileUtils.readFileToString(file, StandardCharsets.UTF_8));
        }
    }

    private void execute_one_query(long queryId, String queryName, String query) throws Exception {
        FileUtils.writeStringToFile(
                logFile,
                String.format(
                        "********************************************************************************************\n"
                            + "%s: %s\n",
                        queryName, query),
                StandardCharsets.UTF_8,
                true);
        GraphBuilder builder = createGraphBuilder(optimizer, ldbcMeta);
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        // apply filter push down optimize
        optimizer.getRelPlanner().setRoot(node);
        node = optimizer.getRelPlanner().findBestExp();
        // apply CBO optimize
        GraphIOProcessor ioProcessor = new GraphIOProcessor(builder, ldbcMeta);
        Random random = new SecureRandom();
        int pickCount = Integer.valueOf(System.getProperty("pick.count"));
        RandomPickOptimizer pickOptimizer =
                new RandomPickOptimizer(
                        ioProcessor,
                        (VolcanoPlanner) optimizer.getMatchPlanner(),
                        random,
                        pickCount);
        RelNode results = node.accept(pickOptimizer);
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
                            new GraphRelProtoPhysicalBuilder(configs, ldbcMeta, logicalPlan)
                                    .build();
                    ExecutionRequest request =
                            new ExecutionRequest(
                                    queryId, "ir_plan_" + queryId, logicalPlan, physicalPlan);
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
                    }
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format(
                                    "execution time %d ms, results: %s\n",
                                    elapsedTime, resultBuilder),
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
        FileUtils.writeStringToFile(logFile, String.format("\n\n\n"), StandardCharsets.UTF_8, true);
    }

    private class RelNodeList extends AbstractRelNode {
        private final List<RelNode> rels;

        public RelNodeList(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> rels) {
            super(cluster, traitSet);
            this.rels = rels;
        }
    }

    private class RandomPickOptimizer extends GraphRelVisitor {
        private final GraphIOProcessor ioProcessor;
        private final VolcanoPlanner matchPlanner;
        private final Random random;
        private final int pickCount;

        public RandomPickOptimizer(
                GraphIOProcessor ioProcessor,
                VolcanoPlanner matchPlanner,
                Random random,
                int pickCount) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
            this.matchPlanner.allSets.clear();
            this.random = random;
            this.pickCount = pickCount;
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
            // add best
            allRels.add(best);
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
            int maxIter = 50;
            RelSet rootSet = allSets.get(0);
            while (randomRels.size() < count && maxIter-- > 0) {
                RelNode randomRel = randomPickOne(matchPlanner, random, rootSet);
                if (!randomDigests.contains(randomRel.getRelDigest())
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
                        intersects.add(rel);
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
}
