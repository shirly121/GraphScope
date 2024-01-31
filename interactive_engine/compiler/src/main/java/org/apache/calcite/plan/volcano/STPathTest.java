package org.apache.calcite.plan.volcano;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.*;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.rel.GraphExtendIntersect;
import com.alibaba.graphscope.common.ir.rel.GraphJoinDecomposition;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.GraphRelVisitor;
import com.alibaba.graphscope.common.ir.rel.graph.match.AbstractLogicalMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.GraphBuilderVisitor;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.tinkerpop.gremlin.driver.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class STPathTest {
    public static final RelDataTypeFactory typeFactory =
            new GraphTypeFactoryImpl(
                    new Configs(
                            ImmutableMap.of(
                                    FrontendConfig.CALCITE_DEFAULT_CHARSET.getKey(), "UTF-8")));
    public static final RexBuilder rexBuilder = new GraphRexBuilder(typeFactory);

    public static void main(String[] args) throws Exception {
        STTest test = new STTest();
        test.run();
        test.close();
    }

    public static class STTest {
        private final Configs configs;
        private final ExecutionClient client;
        private final GraphRelOptimizer optimizer;
        private final IrMeta ldbcMeta;
        private final File logFile;
        private final File queryFile;
        private final Cluster cluster;
        private final Client gremlinClient;
        private final int limit;
        private final List<Integer> skipPlanIds;

        public STTest() throws Exception {
            configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
            queryFile = new File(System.getProperty("query", "query"));
            logFile = new File(System.getProperty("log", "log"));
            if (logFile.exists()) {
                logFile.delete();
            }
            logFile.createNewFile();
            optimizer = new GraphRelOptimizer(new PlannerConfig(configs));
            ldbcMeta = new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
            client = ExecutionClient.Factory.create(configs, new HostsRpcChannelFetcher(configs));
            limit = Integer.valueOf(System.getProperty("limit", "1"));
            int gremlinPort = Integer.valueOf(System.getProperty("gremlin.port", "12312"));
            AuthProperties authProperties = new AuthProperties();
            authProperties
                    .with(AuthProperties.Property.USERNAME, AuthConfig.AUTH_USERNAME.get(configs))
                    .with(AuthProperties.Property.PASSWORD, AuthConfig.AUTH_PASSWORD.get(configs));
            cluster =
                    Cluster.build()
                            .addContactPoint("localhost")
                            .port(gremlinPort)
                            .authProperties(authProperties)
                            .create();
            gremlinClient = cluster.connect();
            String skipPlans = System.getProperty("skip.plans", "");
            skipPlanIds =
                    Utils.convertDotString(skipPlans).stream()
                            .map(Integer::valueOf)
                            .collect(Collectors.toList());
        }

        public void close() throws Exception {
            if (client != null) {
                client.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        }

        public void run() throws Exception {
            File queryFile = new File(System.getProperty("query", "query"));
            String query = FileUtils.readFileToString(queryFile, StandardCharsets.UTF_8);
            List<String> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids);
            query = StringSubstitutor.replace(query, params, "$_", "_");
            execute_one_query(
                    "case_study",
                    query,
                    new Function<GraphIOProcessor, GraphRelVisitor>() {
                        @Override
                        public GraphRelVisitor apply(GraphIOProcessor ioProcessor) {
                            CaseStudyOptimizer pickOptimizer =
                                    new CaseStudyOptimizer(
                                            ioProcessor,
                                            (VolcanoPlanner) optimizer.getMatchPlanner());
                            return pickOptimizer;
                        }
                    });
        }

        private List<String> getRandomPersonIds() {
            String personId =
                    System.getProperty("person.id", "6b715e27848807653966ff7ce6e1b98fP01");
            String query =
                    String.format(
                            "g.V().has('person', 'id', '%s').both().limit(%s).values('id')",
                            personId, limit);
            ResultSet results = gremlinClient.submit(query);
            List<String> ids = Lists.newArrayList();
            for (Result result : results) {
                ids.add("\"" + result.getString() + "\"");
            }
            return ids;
        }

        private void execute_one_query(
                String queryName,
                String query,
                Function<GraphIOProcessor, GraphRelVisitor> visitorFactory)
                throws Exception {
            FileUtils.writeStringToFile(
                    logFile,
                    String.format(
                            "********************************************************************************************\n"
                                + "%s: %s\n",
                            queryName, query),
                    StandardCharsets.UTF_8,
                    true);
            GraphBuilder builder = createGraphBuilder(optimizer, ldbcMeta);
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
                        String logicalExplain =
                                com.alibaba.graphscope.common.ir.tools.Utils.toString(
                                        logicalPlan.getRegularQuery());
                        PhysicalPlan physicalPlan =
                                new GraphRelProtoPhysicalBuilder(configs, ldbcMeta, logicalPlan)
                                        .build();
                        ChannelFetcher fetcher = new HostsRpcChannelFetcher(configs);
                        int workerNum = 32;
                        configs.set(
                                PegasusConfig.PEGASUS_WORKER_NUM.getKey(),
                                String.valueOf(workerNum));
                        ExecutionClient client1 = new RpcExecutionClient(configs, fetcher);
                        int queryId = UUID.randomUUID().hashCode();
                        FileUtils.writeStringToFile(
                                logFile,
                                String.format("logical plan %d: %s\n", i, logicalExplain),
                                StandardCharsets.UTF_8,
                                true);
                        FileUtils.writeStringToFile(
                                new File("physical.log"),
                                String.format(
                                        "plan id: %d, query id: %d, physical plan: %s\n",
                                        i, queryId, physicalPlan.explain()),
                                StandardCharsets.UTF_8,
                                true);
                        if (skipPlanIds.contains(i++)) {
                            FileUtils.writeStringToFile(
                                    logFile,
                                    "skip current logical plan\n",
                                    StandardCharsets.UTF_8,
                                    true);
                            continue;
                        }
                        ExecutionRequest request =
                                new ExecutionRequest(
                                        queryId, "ir_plan_" + queryId, logicalPlan, physicalPlan);
                        long startTime = System.currentTimeMillis();
                        StreamIterator<IrResult.Record> resultIterator = new StreamIterator<>();
                        client1.submit(
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
                                String.format(
                                        "thread num %d, execution time %d ms, results: %s\n",
                                        workerNum, elapsedTime, resultBuilder),
                                StandardCharsets.UTF_8,
                                true);
                        client1.close();
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

        private class CaseStudyOptimizer extends GraphRelVisitor {
            private final GraphIOProcessor ioProcessor;
            private final VolcanoPlanner matchPlanner;

            public CaseStudyOptimizer(GraphIOProcessor ioProcessor, VolcanoPlanner matchPlanner) {
                this.ioProcessor = ioProcessor;
                this.matchPlanner = matchPlanner;
                this.matchPlanner.allSets.clear();
            }

            @Override
            public RelNode visit(GraphLogicalSingleMatch match) {
                return findCaseStudyPlans(match);
            }

            @Override
            public RelNode visit(GraphLogicalMultiMatch match) {
                return findCaseStudyPlans(match);
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

            private RelNode findCaseStudyPlans(AbstractLogicalMatch match) {
                matchPlanner.setRoot(ioProcessor.processInput(match));
                RelNode best = matchPlanner.findBestExp();
                List<RelNode> allRels = Lists.newArrayList();
                Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
                ImmutableList<RelSet> allSets = ordering.immutableSortedCopy(matchPlanner.allSets);
                RelSet rootSet = allSets.get(0);
                // add best
                allRels.add(best);
                // add non opt plans with specific pattern
                List<RelNode> nonOptPlans =
                        enumeratePlans(matchPlanner, rootSet).stream()
                                .filter(
                                        k -> {
                                            CaseStudyVisitor visitor = new CaseStudyVisitor();
                                            visitor.go(k);
                                            return visitor.valid();
                                        })
                                .collect(Collectors.toList());
                int pickCount = Integer.valueOf(System.getProperty("pick.count", "5"));
                nonOptPlans = nonOptPlans.subList(0, Math.min(pickCount, nonOptPlans.size()));
                allRels.addAll(nonOptPlans);
                allRels =
                        allRels.stream()
                                .map(k -> ioProcessor.processOutput(k))
                                .collect(Collectors.toList());
                return new RelNodeList(match.getCluster(), match.getTraitSet(), allRels);
            }

            private class CaseStudyVisitor extends RelVisitor {
                private boolean sourceHasFilter = true;

                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    super.visit(node, ordinal, parent);
                    if (node instanceof GraphPattern) {
                        GraphPattern pattern = (GraphPattern) node;
                        if (pattern.getPattern().getVertexNumber() == 1) {
                            PatternVertex singleVertex =
                                    pattern.getPattern().getVertexSet().iterator().next();
                            if (Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    == 0) {
                                sourceHasFilter = false;
                            }
                        }
                    }
                }

                public boolean valid() {
                    return sourceHasFilter;
                }
            }
        }

        public List<RelNode> enumeratePlans(VolcanoPlanner planner, RelSet root) {
            List<RelNode> plans = Lists.newArrayList();
            for (RelSubset subset : root.subsets) {
                List<RelNode> joinOrIntersect = Lists.newArrayList();
                for (RelNode rel : subset.getRelList()) {
                    if (rel instanceof GraphExtendIntersect
                            || rel instanceof GraphJoinDecomposition) {
                        joinOrIntersect.add(rel);
                    }
                }
                if (joinOrIntersect.isEmpty()) {
                    GraphPattern pattern = (GraphPattern) subset.getOriginal();
                    if (pattern.getPattern().getVertexNumber() == 1) {
                        plans.add(pattern);
                    }
                } else {
                    for (RelNode rel : joinOrIntersect) {
                        List<RelNode> left =
                                enumeratePlans(planner, planner.getSet(rel.getInput(0)));
                        if (rel.getInputs().size() > 1) {
                            List<RelNode> right =
                                    enumeratePlans(planner, planner.getSet(rel.getInput(1)));
                            for (RelNode rel1 : left) {
                                for (RelNode rel2 : right) {
                                    plans.add(
                                            rel.copy(
                                                    rel.getTraitSet(),
                                                    ImmutableList.of(rel1, rel2)));
                                }
                            }
                        } else {
                            for (RelNode rel1 : left) {
                                plans.add(rel.copy(rel.getTraitSet(), ImmutableList.of(rel1)));
                            }
                        }
                    }
                }
            }
            return plans;
        }

        private GraphBuilder createGraphBuilder(GraphRelOptimizer optimizer, IrMeta irMeta) {
            RelOptCluster optCluster =
                    GraphOptCluster.create(optimizer.getMatchPlanner(), rexBuilder);
            optCluster.setMetadataQuerySupplier(() -> optimizer.createMetaDataQuery());
            return (GraphBuilder)
                    GraphPlanner.relBuilderFactory.create(
                            optCluster, new GraphOptSchema(optCluster, irMeta.getSchema()));
        }

        public GraphBuilder eval(String query, GraphBuilder builder) {
            return new GraphBuilderVisitor(builder).visit(new CypherAntlr4Parser().parse(query));
        }
    }
}
