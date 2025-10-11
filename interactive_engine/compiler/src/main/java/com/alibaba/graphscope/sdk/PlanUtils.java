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

package com.alibaba.graphscope.sdk;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.exception.FrontendException;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaCache;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.groot.common.schema.api.GraphElement;
import com.alibaba.graphscope.proto.frontend.Code;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class PlanUtils {
    private static final Logger logger = LoggerFactory.getLogger(PlanUtils.class);
    /**
     * Provide a java-side implementation to compile the query in string to a physical plan
     * @param configPath
     * @param query
     *
     * @return JNIPlan has two fields: physicalBytes and resultSchemaYaml,
     * physicalBytes can be decoded to {@code PhysicalPlan} in c++ side by standard PB serialization,
     * resultSchemaYaml defines the result specification of the query in yaml format
     * @throws Exception
     */
    public static GraphPlan compilePlan(
            String configPath, long version, String query, String schemaYaml, String statsJson) {
        StringBuilder msgBuilder = new StringBuilder();
        try {
            long startTime = System.currentTimeMillis();
            Configs configs = Configs.Factory.create(configPath);
            GraphPlanner graphPlanner = GraphPlanerInstance.getInstance(configs);
            IrMetaCache metaCache = IrMetaCache.getInstance(configs);
            IrMeta irMeta =
                    metaCache.get(
                            new IrMetaCache.Key(
                                    version,
                                    configs,
                                    schemaYaml,
                                    statsJson,
                                    graphPlanner.getOptimizer().getGlogueHolder()));
            msgBuilder.append("\nparamLabels: [ " + printLabels(irMeta.getSchema()) + " ]\n");
            GraphPlanner.PlannerInstance plannerInstance =
                    graphPlanner.instance(query, irMeta, null, msgBuilder);
            GraphPlanner.Summary summary = plannerInstance.plan(msgBuilder);
            LogicalPlan logicalPlan = summary.getLogicalPlan();
            PhysicalPlan<byte[]> physicalPlan = summary.getPhysicalPlan();
            StoredProcedureMeta procedureMeta =
                    new StoredProcedureMeta(
                            new Configs(ImmutableMap.of()),
                            query,
                            logicalPlan.getOutputType(),
                            logicalPlan.getDynamicParams());
            ByteArrayOutputStream metaStream = new ByteArrayOutputStream();
            StoredProcedureMeta.Serializer.perform(procedureMeta, metaStream, false);
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("compile plan cost: {} ms", elapsedTime);
            return new GraphPlan(
                    Code.OK,
                    msgBuilder.toString(),
                    physicalPlan.getContent(),
                    new String(metaStream.toByteArray()));
        } catch (Throwable t) {
            if (t instanceof FrontendException) {
                String errorMsg = t.getMessage();
                errorMsg += "\nExtraMsg: " + msgBuilder;
                if (((FrontendException) t).getDetails() != null
                        && ((FrontendException) t).getDetails().get("stacktrace") != null) {
                    errorMsg +=
                            "\nStacktrace: "
                                    + ((FrontendException) t).getDetails().get("stacktrace");
                }
                return new GraphPlan(((FrontendException) t).getErrorCode(), errorMsg, null, null);
            }
            return new GraphPlan(Code.UNRECOGNIZED, t.getMessage(), null, null);
        }
    }

    public static GraphPlan compilePlan(
            String configPath, String query, String schemaYaml, String statsJson) {
        return compilePlan(configPath, 0, query, schemaYaml, statsJson);
    }

    public static Map<String, Object> printLabels(IrGraphSchema schema) {
        try {
            GraphElement process = schema.getElement("process");
            GraphElement ip = schema.getElement("ip");
            GraphElement access = schema.getElement("access");
            GraphElement servers = schema.getElement("servers");
            return ImmutableMap.of(
                    "process",
                    process.getLabelId(),
                    "ip",
                    ip.getLabelId(),
                    "access",
                    access.getLabelId(),
                    "servers",
                    servers.getLabelId());
        } catch (Exception e) {
            return ImmutableMap.of();
        }
    }
}
