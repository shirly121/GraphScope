package org.apache.calcite.plan.volcano;

import static com.alibaba.graphscope.common.ir.glogue.LdbcTest.createGraphBuilder;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.*;
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
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueExtendIntersectEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RandomPickPlanTest {
    private static Configs configs;
    private static ExecutionClient client;
    private static GraphRelOptimizer optimizer;
    private static IrMeta ldbcMeta;
    private static File logFile;
    private static File queryDir;
    private static Session session;
    private static int limit;

    @BeforeClass
    public static void beforeClass() throws Exception {
        configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        System.out.println(configs);
        queryDir = new File(System.getProperty("query", "queries/ldbc"));
        Preconditions.checkArgument(
                queryDir.exists() && queryDir.isDirectory(),
                queryDir + " is not a valid directory");
        logFile = new File(System.getProperty("log", "log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
        optimizer = new GraphRelOptimizer(new PlannerConfig(configs));
        ldbcMeta = new ExperimentalMetaFetcher(new LocalMetaDataReader(configs)).fetch().get();
        client = ExecutionClient.Factory.create(configs, new HostsRpcChannelFetcher(configs));
        limit = Integer.valueOf(System.getProperty("limit", "10"));
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
    }

    @Test
    public void run_test() throws Exception {
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            execute_one_query(
                    file.getName(),
                    FileUtils.readFileToString(file, StandardCharsets.UTF_8),
                    new Function<GraphIOProcessor, GraphRelVisitor>() {
                        @Override
                        public GraphRelVisitor apply(GraphIOProcessor ioProcessor) {
                            Random random = new SecureRandom();
                            int pickCount = Integer.valueOf(System.getProperty("pick.count", "1"));
                            RandomPickOptimizer pickOptimizer =
                                    new RandomPickOptimizer(
                                            ioProcessor,
                                            (VolcanoPlanner) optimizer.getMatchPlanner(),
                                            random,
                                            pickCount);
                            return pickOptimizer;
                        }
                    });
        }
    }

    @Test
    public void run_ldbc_3_0_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_3"), StandardCharsets.UTF_8);
        String countryX = "\"Laos\"";
        String countryY = "\"United_States\"";
        List<Long> ids = getRandomPersonIds();
        Map<String, Object> params =
                ImmutableMap.of(
                        "id",
                        ids,
                        "xName",
                        countryX,
                        "yName",
                        countryY,
                        "date1",
                        20100505013715278L,
                        "date2",
                        20130604130807720L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        execute_one_query(
                "ldbc_3_0",
                query,
                new Function<GraphIOProcessor, GraphRelVisitor>() {
                    @Override
                    public GraphRelVisitor apply(GraphIOProcessor ioProcessor) {
                        return new Ldbc3NonOptimizer(
                                ioProcessor, (VolcanoPlanner) optimizer.getMatchPlanner(), 0);
                    }
                });
    }

    @Test
    public void run_ldbc_3_1_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_3_1"), StandardCharsets.UTF_8);
        String countryX = "\"Laos\"";
        String countryY = "\"United_States\"";
        List<Long> ids = getRandomPersonIds();
        Map<String, Object> params =
                ImmutableMap.of(
                        "id",
                        ids,
                        "xName",
                        countryX,
                        "yName",
                        countryY,
                        "date1",
                        20100505013715278L,
                        "date2",
                        20130604130807720L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        execute_one_query(
                "ldbc_3_1",
                query,
                new Function<GraphIOProcessor, GraphRelVisitor>() {
                    @Override
                    public GraphRelVisitor apply(GraphIOProcessor ioProcessor) {
                        return new Ldbc3NonOptimizer(
                                ioProcessor, (VolcanoPlanner) optimizer.getMatchPlanner(), 1);
                    }
                });
    }

    @Test
    public void run_ldbc_5_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_5"), StandardCharsets.UTF_8);
        List<Long> ids = getRandomPersonIds();
        Map<String, Object> params = ImmutableMap.of("id", ids, "date", 20100325000000000L);
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        execute_one_query(
                "ldbc_5",
                query,
                new Function<GraphIOProcessor, GraphRelVisitor>() {
                    @Override
                    public GraphRelVisitor apply(GraphIOProcessor ioProcessor) {
                        return new Ldbc5NonOptimizer(
                                ioProcessor, (VolcanoPlanner) optimizer.getMatchPlanner());
                    }
                });
    }

    @Test
    public void run_ldbc_6_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_6"), StandardCharsets.UTF_8);
        List<Long> ids = getRandomPersonIds();
        Map<String, Object> params =
                ImmutableMap.of("id", ids, "tag", "\"North_German_Confederation\"");
        String query = StringSubstitutor.replace(template, params, "$_", "_");
        execute_one_query(
                "ldbc_6",
                query,
                new Function<GraphIOProcessor, GraphRelVisitor>() {
                    @Override
                    public GraphRelVisitor apply(GraphIOProcessor ioProcessor) {
                        return new Ldbc6NonOptimizer(
                                ioProcessor, (VolcanoPlanner) optimizer.getMatchPlanner());
                    }
                });
    }

    @Test
    public void run_case_study_test() throws Exception {
        String query =
                "Match (p1:PERSON {id: $_id_})-[:KNOWS*1..5]->(p2:PERSON {id: $_id_}) Return"
                        + " count(p1)";
        List<Long> ids = getRandomPersonIds();
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
                                        ioProcessor, (VolcanoPlanner) optimizer.getMatchPlanner());
                        return pickOptimizer;
                    }
                });
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
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
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
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format("logical plan %d: %s\n", i++, logicalExplain),
                            StandardCharsets.UTF_8,
                            true);
                    ChannelFetcher fetcher = new HostsRpcChannelFetcher(configs);
                    int totalNum = Integer.valueOf(System.getProperty("thread.num", "32"));
                    for (int workerNum = 32; workerNum <= totalNum; workerNum *= 2) {
                        configs.set(
                                PegasusConfig.PEGASUS_WORKER_NUM.getKey(),
                                String.valueOf(workerNum));
                        ExecutionClient client1 = new RpcExecutionClient(configs, fetcher);
                        PhysicalPlan physicalPlan =
                                new GraphRelProtoPhysicalBuilder(configs, ldbcMeta, logicalPlan)
                                        .build();
                        int queryId = UUID.randomUUID().hashCode();
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
                        if (Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d) < 0) {
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

    private class Ldbc3NonOptimizer extends GraphRelVisitor {
        private final GraphIOProcessor ioProcessor;
        private final VolcanoPlanner matchPlanner;
        private final int queryId;

        public Ldbc3NonOptimizer(
                GraphIOProcessor ioProcessor, VolcanoPlanner matchPlanner, int queryId) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
            this.matchPlanner.allSets.clear();
            this.queryId = queryId;
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return findBestWithNonOpt(match);
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return findBestWithNonOpt(match);
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

        private RelNode findBestWithNonOpt(AbstractLogicalMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            RelNode best = matchPlanner.findBestExp();
            List<RelNode> allRels = Lists.newArrayList();
            Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
            ImmutableList<RelSet> allSets = ordering.immutableSortedCopy(matchPlanner.allSets);
            RelSet rootSet = allSets.get(0);
            // add best
            // allRels.add(best);
            // add non opt plans with specific pattern
            List<RelNode> nonOptPlans =
                    enumeratePlans(matchPlanner, rootSet).stream()
                            .filter(
                                    k -> {
                                        NonOptPlanVisitor visitor = new NonOptPlanVisitor();
                                        visitor.go(k);
                                        return visitor.valid();
                                    })
                            .collect(Collectors.toList());
            int pickCount = Integer.valueOf(System.getProperty("pick.count", "1"));
            nonOptPlans = nonOptPlans.subList(0, Math.min(pickCount, nonOptPlans.size()));
            allRels.addAll(nonOptPlans);
            allRels =
                    allRels.stream()
                            .map(k -> ioProcessor.processOutput(k))
                            .collect(Collectors.toList());
            return new RelNodeList(match.getCluster(), match.getTraitSet(), allRels);
        }

        private class NonOptPlanVisitor extends RelVisitor {
            private boolean hasJoin;
            private boolean placeAsSource;
            private int expandFromCountryXCount;
            private boolean cityXAsSource;
            private boolean personAsSource;
            private int placeAsSourceCount;
            private int joinCount;
            private boolean tagHasFilter;
            private boolean isPersonPost;
            private boolean joinAtPerson = true;

            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                super.visit(node, ordinal, parent);
                if (node instanceof GraphPattern) {
                    GraphPattern pattern = (GraphPattern) node;
                    if (isCountryX(pattern.getPattern())) {
                        placeAsSource = true;
                        placeAsSourceCount++;
                    }
                    if (isPersonWithFilter(pattern.getPattern())) {
                        personAsSource = true;
                    }
                    PatternVertex singleVertex =
                            pattern.getPattern().getVertexSet().iterator().next();
                    if (singleVertex.getVertexTypeIds().get(0) == 7
                            && Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    < 0) {
                        tagHasFilter = true;
                    }

                } else if (node instanceof GraphExtendIntersect) {
                    if (isPersonCity(
                            ((GraphExtendIntersect) node).getGlogueEdge().getDstPattern())) {
                        if (node.getInput(0) instanceof GraphPattern) {
                            cityXAsSource = true;
                        }
                    }
                    GlogueExtendIntersectEdge glogueEdge =
                            ((GraphExtendIntersect) node).getGlogueEdge();
                    List<PatternVertex> srcVertices =
                            glogueEdge.getExtendStep().getExtendEdges().stream()
                                    .map(
                                            k ->
                                                    glogueEdge
                                                            .getSrcPattern()
                                                            .getVertexByOrder(
                                                                    k.getSrcVertexOrder()))
                                    .collect(Collectors.toList());
                    if (containsCountryX(glogueEdge.getSrcPattern())
                            && srcVertices.stream()
                                    .allMatch(k -> k.getVertexTypeIds().get(0) == 0)) {
                        expandFromCountryXCount++;
                    }
                } else if (node instanceof GraphJoinDecomposition) {
                    GraphJoinDecomposition join = (GraphJoinDecomposition) node;
                    if (isPersonPerson(join.getProbePattern())) {
                        hasJoin = true;
                    }
                    List<PatternVertex> jointVertices =
                            join.getJoinVertexPairs().stream()
                                    .map(
                                            k -> {
                                                return join.getProbePattern()
                                                        .getVertexByOrder(k.getLeftOrderId());
                                            })
                                    .collect(Collectors.toList());
                    if (jointVertices.stream().anyMatch(k -> k.getVertexTypeIds().get(0) != 1)) {
                        joinAtPerson = false;
                    }
                    ++joinCount;
                }
            }

            private boolean isPersonWithFilter(Pattern pattern) {
                if (pattern.getVertexNumber() == 1) {
                    PatternVertex singleVertex = pattern.getVertexSet().iterator().next();
                    if (singleVertex.getVertexTypeIds().get(0) == 1
                            && Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    < 0) {
                        return true;
                    }
                }
                return false;
            }

            private boolean isCountryX(Pattern pattern) {
                if (pattern.getVertexNumber() == 1) {
                    PatternVertex singleVertex = pattern.getVertexSet().iterator().next();
                    if (singleVertex.getVertexTypeIds().get(0) == 0
                            && Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    < 0) {
                        return true;
                    }
                }
                return false;
            }

            private boolean containsCountryX(Pattern pattern) {
                if (pattern.getVertexNumber() == 2) {
                    return pattern.getVertexSet().stream()
                            .allMatch(
                                    k -> {
                                        if (k.isDistinct()) {
                                            return k.getVertexTypeIds().get(0) == 0;
                                        } else {
                                            return k.getVertexTypeIds()
                                                    .containsAll(ImmutableList.of(2, 3));
                                        }
                                    });
                } else {
                    return isCountryX(pattern);
                }
            }

            private boolean isPersonCity(Pattern p) {
                return p.getVertexNumber() == 2
                        && p.getVertexSet().stream()
                                .allMatch(
                                        k -> {
                                            int typeId = k.getVertexTypeIds().get(0);
                                            return typeId == 0 || typeId == 1;
                                        });
            }

            private boolean isPersonPerson(Pattern p) {
                return p.getVertexNumber() == 2
                        && p.getVertexSet().stream()
                                .allMatch(k -> k.getVertexTypeIds().get(0) == 1);
            }

            private boolean isPersonPost(Pattern p) {
                return p.getVertexNumber() == 3
                        && p.getVertexSet().stream()
                                .allMatch(
                                        k -> {
                                            int typeId = k.getVertexTypeIds().get(0);
                                            return typeId == 1 || typeId == 3;
                                        });
            }

            public boolean valid() {
                // return placeAsSource && hasJoin && personAsSource;
                // return (hasJoin && personAsSource && tagHasFilter);
                return queryId == 0
                                && joinCount == 2
                                && placeAsSourceCount == 2
                                && joinAtPerson
                                && personAsSource
                        || (queryId == 1 && placeAsSource);
            }
        }
    }

    private class Ldbc6NonOptimizer extends GraphRelVisitor {
        private final GraphIOProcessor ioProcessor;
        private final VolcanoPlanner matchPlanner;

        public Ldbc6NonOptimizer(GraphIOProcessor ioProcessor, VolcanoPlanner matchPlanner) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
            this.matchPlanner.allSets.clear();
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return findBestWithNonOpt(match);
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return findBestWithNonOpt(match);
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

        private RelNode findBestWithNonOpt(AbstractLogicalMatch match) {
            matchPlanner.setRoot(ioProcessor.processInput(match));
            RelNode best = matchPlanner.findBestExp();
            List<RelNode> allRels = Lists.newArrayList();
            Ordering<RelSet> ordering = Ordering.from(Comparator.comparingInt(o -> o.id));
            ImmutableList<RelSet> allSets = ordering.immutableSortedCopy(matchPlanner.allSets);
            RelSet rootSet = allSets.get(0);
            // add best
            // allRels.add(best);
            // add non opt plans with specific pattern
            List<RelNode> nonOptPlans =
                    enumeratePlans(matchPlanner, rootSet).stream()
                            .filter(
                                    k -> {
                                        NonOptPlanVisitor visitor = new NonOptPlanVisitor();
                                        visitor.go(k);
                                        return visitor.valid();
                                    })
                            .collect(Collectors.toList());
            int pickCount = Integer.valueOf(System.getProperty("pick.count", "2"));
            nonOptPlans = nonOptPlans.subList(0, Math.min(pickCount, nonOptPlans.size()));
            allRels.addAll(nonOptPlans);
            allRels =
                    allRels.stream()
                            .map(k -> ioProcessor.processOutput(k))
                            .collect(Collectors.toList());
            return new RelNodeList(match.getCluster(), match.getTraitSet(), allRels);
        }

        private class NonOptPlanVisitor extends RelVisitor {
            private boolean personAsSource;
            private boolean tagAsSource;
            private int joinCount;

            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                super.visit(node, ordinal, parent);
                if (node instanceof GraphPattern) {
                    GraphPattern pattern = (GraphPattern) node;
                    if (isPersonWithFilter(pattern.getPattern())) {
                        personAsSource = true;
                    }
                    if (isTagWithoutFilter(pattern.getPattern())) {
                        tagAsSource = true;
                    }
                } else if (node instanceof GraphJoinDecomposition) {
                    joinCount++;
                }
            }

            private boolean isPersonWithFilter(Pattern pattern) {
                if (pattern.getVertexNumber() == 1) {
                    PatternVertex singleVertex = pattern.getVertexSet().iterator().next();
                    if (singleVertex.getVertexTypeIds().get(0) == 1
                            && Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    < 0) {
                        return true;
                    }
                }
                return false;
            }

            private boolean isTagWithoutFilter(Pattern pattern) {
                if (pattern.getVertexNumber() == 1) {
                    PatternVertex singleVertex = pattern.getVertexSet().iterator().next();
                    if (singleVertex.getVertexTypeIds().get(0) == 7
                            && Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    < 0) {
                        return true;
                    }
                }
                return false;
            }

            boolean valid() {
                // return personAsSource && joinCount == 1 && tagAsSource;
                return tagAsSource && joinCount == 0;
            }
        }
    }

    private class Ldbc5NonOptimizer extends GraphRelVisitor {
        private final GraphIOProcessor ioProcessor;
        private final VolcanoPlanner matchPlanner;

        public Ldbc5NonOptimizer(GraphIOProcessor ioProcessor, VolcanoPlanner matchPlanner) {
            this.ioProcessor = ioProcessor;
            this.matchPlanner = matchPlanner;
            this.matchPlanner.allSets.clear();
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            return findBestWithNonOpt(match);
        }

        @Override
        public RelNode visit(GraphLogicalMultiMatch match) {
            return findBestWithNonOpt(match);
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

        private RelNode findBestWithNonOpt(AbstractLogicalMatch match) {
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
                                        NonOptPlanVisitor visitor = new NonOptPlanVisitor();
                                        visitor.go(k);
                                        return visitor.valid();
                                    })
                            .collect(Collectors.toList());
            int pickCount = Integer.valueOf(System.getProperty("pick.count", "2"));
            nonOptPlans = nonOptPlans.subList(0, Math.min(pickCount, nonOptPlans.size()));
            allRels.addAll(nonOptPlans);
            allRels =
                    allRels.stream()
                            .map(k -> ioProcessor.processOutput(k))
                            .collect(Collectors.toList());
            return new RelNodeList(match.getCluster(), match.getTraitSet(), allRels);
        }

        private class NonOptPlanVisitor extends RelVisitor {
            private boolean personAsSource;
            private int joinCount;

            @Override
            public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                super.visit(node, ordinal, parent);
                if (node instanceof GraphPattern) {
                    GraphPattern pattern = (GraphPattern) node;
                    if (isPersonWithFilter(pattern.getPattern())) {
                        personAsSource = true;
                    }
                } else if (node instanceof GraphJoinDecomposition) {
                    joinCount++;
                }
            }

            private boolean isPersonWithFilter(Pattern pattern) {
                if (pattern.getVertexNumber() == 1) {
                    PatternVertex singleVertex = pattern.getVertexSet().iterator().next();
                    if (singleVertex.getVertexTypeIds().get(0) == 1
                            && Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d)
                                    < 0) {
                        return true;
                    }
                }
                return false;
            }

            boolean valid() {
                return personAsSource;
            }
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
                    if (Double.compare(singleVertex.getDetails().getSelectivity(), 1.0d) < 0) {
                        sourceHasFilter = true;
                    }
                }
            }
        }

        public boolean isSourceHasFilter() {
            return sourceHasFilter;
        }
    }

    private class JoinVisitor extends RelVisitor {
        private boolean hasJoin = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            super.visit(node, ordinal, parent);
            if (node instanceof RelSubset
                    && ((RelSubset) node).getBest() instanceof GraphJoinDecomposition) {
                hasJoin = true;
            }
        }
    }

    private List<Long> getRandomPersonIds() {
        String query = String.format("Match (p:PERSON) Return p.id as id limit %d", limit);
        Result result = session.run(query);
        List<Long> ids = Lists.newArrayList();
        while (result.hasNext()) {
            ids.add(result.next().get("id").asLong());
        }
        return ids;
        // return ImmutableList.of(1L, 2L);
    }

    public List<RelNode> enumeratePlans(VolcanoPlanner planner, RelSet root) {
        List<RelNode> plans = Lists.newArrayList();
        for (RelSubset subset : root.subsets) {
            List<RelNode> joinOrIntersect = Lists.newArrayList();
            for (RelNode rel : subset.getRelList()) {
                if (rel instanceof GraphExtendIntersect || rel instanceof GraphJoinDecomposition) {
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
                    List<RelNode> left = enumeratePlans(planner, planner.getSet(rel.getInput(0)));
                    if (rel.getInputs().size() > 1) {
                        List<RelNode> right =
                                enumeratePlans(planner, planner.getSet(rel.getInput(1)));
                        for (RelNode rel1 : left) {
                            for (RelNode rel2 : right) {
                                plans.add(
                                        rel.copy(rel.getTraitSet(), ImmutableList.of(rel1, rel2)));
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
}
