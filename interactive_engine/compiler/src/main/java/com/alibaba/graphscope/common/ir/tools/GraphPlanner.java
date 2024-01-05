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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.reader.LocalMetaDataReader;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.ProcedurePhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.ffi.FfiPhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.proto.GraphRelProtoPhysicalBuilder;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A unified structure to build {@link PlannerInstance} which can further build logical and physical plan from an antlr tree
 */
public class GraphPlanner {
    private static final Logger logger = LoggerFactory.getLogger(GraphPlanner.class);
    private final Configs graphConfig;
    private final PlannerConfig plannerConfig;
    private final GraphRelOptimizer optimizer;
    private final RexBuilder rexBuilder;
    private final AtomicLong idGenerator;
    private final LogicalPlanFactory logicalPlanFactory;

    public static final RelBuilderFactory relBuilderFactory =
            (RelOptCluster cluster, @Nullable RelOptSchema schema) ->
                    GraphBuilder.create(null, (GraphOptCluster) cluster, schema);
    public static final Function<Configs, RexBuilder> rexBuilderFactory =
            (Configs configs) -> new GraphRexBuilder(new GraphTypeFactoryImpl(configs));

    public GraphPlanner(Configs graphConfig, LogicalPlanFactory logicalPlanFactory) {
        this.graphConfig = graphConfig;
        this.plannerConfig = PlannerConfig.create(this.graphConfig);
        logger.debug("planner config: " + this.plannerConfig);
        this.optimizer = new GraphRelOptimizer(this.plannerConfig);
        this.rexBuilder = rexBuilderFactory.apply(graphConfig);
        this.idGenerator = new AtomicLong(FrontendConfig.FRONTEND_SERVER_ID.get(graphConfig));
        this.logicalPlanFactory = logicalPlanFactory;
    }

    public PlannerInstance instance(String query, IrMeta irMeta) {
        GraphOptCluster optCluster =
                GraphOptCluster.create(this.optimizer.getMatchPlanner(), this.rexBuilder);
        RelMetadataQuery mq = optimizer.createMetaDataQuery();
        if (mq != null) {
            optCluster.setMetadataQuerySupplier(() -> mq);
        }
        return new PlannerInstance(query, optCluster, irMeta);
    }

    public long generateUniqueId() {
        long delta = FrontendConfig.FRONTEND_SERVER_NUM.get(graphConfig);
        return idGenerator.getAndAdd(delta);
    }

    public String generateUniqueName(long uniqueId) {
        return "ir_plan_" + uniqueId;
    }

    public class PlannerInstance {
        private final String query;
        private final GraphOptCluster optCluster;
        private final IrMeta irMeta;

        public PlannerInstance(String query, GraphOptCluster optCluster, IrMeta irMeta) {
            this.query = query;
            this.optCluster = optCluster;
            this.irMeta = irMeta;
        }

        public Summary plan() {
            long compileStartTime = System.currentTimeMillis();
            long jobId = generateUniqueId();
            Pair<LogicalPlan, Long> logicalPlanOpt = planLogicalOpt();
            PhysicalPlan physicalPlan = planPhysical(logicalPlanOpt.getValue0());
            long compileElapsed = System.currentTimeMillis() - compileStartTime;
            return new Summary(
                    jobId,
                    generateUniqueName(jobId),
                    logicalPlanOpt.getValue0(),
                    physicalPlan,
                    compileElapsed,
                    logicalPlanOpt.getValue1());
        }

        public LogicalPlan planLogical() {
            // build logical plan from parsed query
            IrGraphSchema schema = irMeta.getSchema();
            GraphBuilder graphBuilder =
                    GraphBuilder.create(
                            null, this.optCluster, new GraphOptSchema(this.optCluster, schema));
            LogicalPlan logicalPlan = logicalPlanFactory.create(graphBuilder, irMeta, query);
            // apply optimizations
            if (logicalPlan.getRegularQuery() != null && !logicalPlan.isReturnEmpty()) {
                RelNode before = logicalPlan.getRegularQuery();
                RelNode after =
                        optimizer.optimize(before, new GraphIOProcessor(graphBuilder, irMeta));
                if (after != before) {
                    logicalPlan = new LogicalPlan(after, logicalPlan.getDynamicParams());
                }
            }
            return logicalPlan;
        }

        public Pair<LogicalPlan, Long> planLogicalOpt() {
            // build logical plan from parsed query
            IrGraphSchema schema = irMeta.getSchema();
            GraphBuilder graphBuilder =
                    GraphBuilder.create(
                            null, this.optCluster, new GraphOptSchema(this.optCluster, schema));
            LogicalPlan logicalPlan = logicalPlanFactory.create(graphBuilder, irMeta, query);
            long optStartTime = System.currentTimeMillis();
            // apply optimizations
            if (logicalPlan.getRegularQuery() != null && !logicalPlan.isReturnEmpty()) {
                RelNode before = logicalPlan.getRegularQuery();
                RelNode after =
                        optimizer.optimize(before, new GraphIOProcessor(graphBuilder, irMeta));
                if (after != before) {
                    logicalPlan = new LogicalPlan(after, logicalPlan.getDynamicParams());
                }
            }
            logger.info("cypher query \"{}\", logical plan\n {}", query, logicalPlan.explain());
            long optElapsed = System.currentTimeMillis() - optStartTime;
            return Pair.with(logicalPlan, optElapsed);
        }

        public PhysicalPlan planPhysical(LogicalPlan logicalPlan) {
            // build physical plan from logical plan
            if (logicalPlan.isReturnEmpty()) {
                return PhysicalPlan.createEmpty();
            } else if (logicalPlan.getRegularQuery() != null) {
                String physicalOpt = FrontendConfig.PHYSICAL_OPT_CONFIG.get(graphConfig);
                if ("proto".equals(physicalOpt.toLowerCase())) {
                    logger.info("physical type is proto");
                    try (GraphRelProtoPhysicalBuilder physicalBuilder =
                            new GraphRelProtoPhysicalBuilder(graphConfig, irMeta, logicalPlan)) {
                        return physicalBuilder.build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    logger.info("physical type is ffi");
                    try (PhysicalBuilder physicalBuilder =
                            new FfiPhysicalBuilder(graphConfig, irMeta, logicalPlan)) {
                        return physicalBuilder.build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                return new ProcedurePhysicalBuilder(logicalPlan).build();
            }
        }
    }

    public static class Summary {
        private final long id;
        private final String name;
        private final LogicalPlan logicalPlan;
        private final PhysicalPlan physicalPlan;
        private final long compileTime;
        private final long optTime;

        public Summary(long id, String name, LogicalPlan logicalPlan, PhysicalPlan physicalPlan) {
            this.id = id;
            this.name = name;
            this.logicalPlan = Objects.requireNonNull(logicalPlan);
            this.physicalPlan = Objects.requireNonNull(physicalPlan);
            this.compileTime = 0l;
            this.optTime = 0l;
        }

        public Summary(
                long id,
                String name,
                LogicalPlan logicalPlan,
                PhysicalPlan physicalPlan,
                long compileTime,
                long optTime) {
            this.id = id;
            this.name = name;
            this.logicalPlan = Objects.requireNonNull(logicalPlan);
            this.physicalPlan = Objects.requireNonNull(physicalPlan);
            this.compileTime = compileTime;
            this.optTime = optTime;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public PhysicalPlan getPhysicalPlan() {
            return physicalPlan;
        }

        // total compile time : parser + logical plan with opt + physical plan
        public long getCompileTime() {
            return compileTime;
        }

        // logical plan opt time
        public long getOptTime() {
            return optTime;
        }
    }

    private static Configs createExtraConfigs(@Nullable String keyValues) {
        Map<String, String> keyValueMap = Maps.newHashMap();
        if (!StringUtils.isEmpty(keyValues)) {
            String[] pairs = keyValues.split(",");
            for (String pair : pairs) {
                String[] kv = pair.trim().split(":");
                Preconditions.checkArgument(
                        kv.length == 2, "invalid key value pair: " + pair + " in " + keyValues);
                keyValueMap.put(kv[0], kv[1]);
            }
        }
        return new Configs(keyValueMap);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4
                || args[0].isEmpty()
                || args[1].isEmpty()
                || args[2].isEmpty()
                || args[3].isEmpty()) {
            throw new IllegalArgumentException(
                    "usage: GraphPlanner '<path_to_config_file>' '<path_to_query_file>' "
                            + " '<path_to_physical_output_file>' '<path_to_procedure_file>'"
                            + " 'optional <extra_key_value_config_pairs>'");
        }
        Configs configs = Configs.Factory.create(args[0]);
        ExperimentalMetaFetcher metaFetcher =
                new ExperimentalMetaFetcher(new LocalMetaDataReader(configs));
        String query = FileUtils.readFileToString(new File(args[1]), StandardCharsets.UTF_8);
        GraphPlanner planner =
                new GraphPlanner(
                        configs,
                        (GraphBuilder builder, IrMeta irMeta, String q) ->
                                new LogicalPlanVisitor(builder, irMeta)
                                        .visit(new CypherAntlr4Parser().parse(q)));
        PlannerInstance instance = planner.instance(query, metaFetcher.fetch().get());
        Summary summary = instance.plan();
        // write physical plan to file
        PhysicalPlan<byte[]> physicalPlan = summary.physicalPlan;
        FileUtils.writeByteArrayToFile(new File(args[2]), physicalPlan.getContent());
        // write stored procedure meta to file
        LogicalPlan logicalPlan = summary.getLogicalPlan();
        Configs extraConfigs = createExtraConfigs(args.length > 4 ? args[4] : null);
        StoredProcedureMeta procedureMeta =
                new StoredProcedureMeta(
                        extraConfigs, logicalPlan.getOutputType(), logicalPlan.getDynamicParams());
        StoredProcedureMeta.Serializer.perform(procedureMeta, new FileOutputStream(args[3]));
    }
}
