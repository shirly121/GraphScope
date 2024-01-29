package com.alibaba.graphscope.common.ir.glogue;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class FieldTrimTest {
    private static Configs configs;
    private static ExecutionClient client;
    private static File logFile;

    @BeforeClass
    public static void beforeClass() throws Exception {
        configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        logFile = new File(System.getProperty("log", "log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
        client = ExecutionClient.Factory.create(configs, new HostsRpcChannelFetcher(configs));
    }

    @Test
    public void run_physical_plans() throws Exception {
        List<String> jsonPlans =
                Lists.newArrayList(
                        FileUtils.readFileToString(
                                new File("conf/field_trim_plans/p2/baseline.json"),
                                StandardCharsets.UTF_8),
                        FileUtils.readFileToString(
                                new File("conf/field_trim_plans/p2/trim_aliases.json"),
                                StandardCharsets.UTF_8));
        int i = 0;
        for (String json : jsonPlans) {
            try {
                GraphAlgebraPhysical.PhysicalPlan.Builder planBuilder =
                        GraphAlgebraPhysical.PhysicalPlan.newBuilder();
                JsonFormat.parser().merge(json, planBuilder);
                PhysicalPlan physicalPlan =
                        new PhysicalPlan(planBuilder.build().toByteArray(), json);
                int queryId = UUID.randomUUID().hashCode();
                ExecutionRequest request =
                        new ExecutionRequest(queryId, "ir_plan_" + queryId, null, physicalPlan);
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
                                "physical plan: num %d, execution time %d ms, results: %s\n",
                                i, elapsedTime, resultBuilder),
                        StandardCharsets.UTF_8,
                        true);
                ++i;
            } catch (Exception e) {
                FileUtils.writeStringToFile(
                        logFile,
                        String.format("execution exception %s\n", e.getMessage()),
                        StandardCharsets.UTF_8,
                        true);
            }
        }
    }

    @Test
    public void field_trim_test() {
        Configs configs1 =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterMatchRule, ExtendIntersectRule," + " ExpandGetVFusionRule",
                                "graph.planner.cbo.glogue.schema",
                                "conf/ldbc30_hierarchy_statistics.txt",
                                "graph.planner.join.min.pattern.size",
                                "4"));
        GraphRelOptimizer optimizer = new GraphRelOptimizer(new PlannerConfig(configs1));
        IrMeta ldbcMeta = Utils.mockSchemaMeta("schema/ldbc_schema_exp_hierarchy.json");
        GraphBuilder builder = LdbcTest.createGraphBuilder(optimizer, ldbcMeta);
        String query =
                "Match (p:COMMENT)-[]->(p2:PERSON)-[]->(c:CITY),\n"
                        + "    \t(p)<-[]-(message),\n"
                        + "      (message)-[]->(tag:TAG)\n"
                        + "Return count(tag);";
        RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, ldbcMeta));
        // System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        PhysicalPlan physicalPlan =
                new GraphRelProtoPhysicalBuilder(configs1, ldbcMeta, new LogicalPlan(after))
                        .build();
        System.out.println(physicalPlan.explain());
    }
}
