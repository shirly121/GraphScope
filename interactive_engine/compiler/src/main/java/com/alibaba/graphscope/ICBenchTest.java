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

package com.alibaba.graphscope;

import com.alibaba.graphscope.common.client.ExecutionClient;
import com.alibaba.graphscope.common.client.RpcExecutionClient;
import com.alibaba.graphscope.common.client.channel.HostsRpcChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlanFactory;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.pegasus.common.StreamIterator;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ICBenchTest {
    private final Configs configs;
    private final IrMeta irMeta;
    private final GraphRelOptimizer optimizer;
    private final GraphPlanner planner;
    private final File queryDir;
    private final File queryLog;
    private final File errorLog;
    private final ExecutionClient client;

    public ICBenchTest(Configs configs, File queryDir) throws Exception {
        this.configs = configs;
        this.optimizer = new GraphRelOptimizer(configs);
        this.planner = new GraphPlanner(configs, new LogicalPlanFactory.Cypher(), optimizer);
        this.irMeta =
                new StaticIrMetaFetcher(new LocalIrMetaReader(configs), optimizer.getGlogueHolder())
                        .fetch()
                        .get();
        this.client = new RpcExecutionClient(configs, new HostsRpcChannelFetcher(configs));
        this.queryDir = queryDir;
        this.queryLog = new File(Path.of(queryDir.getAbsolutePath(), "query.log").toString());
        if (queryLog.exists()) {
            queryLog.delete();
        }
        this.errorLog = new File(Path.of(queryDir.getAbsolutePath(), "query.error").toString());
        if (errorLog.exists()) {
            errorLog.delete();
        }
    }

    public GraphPlanner.Summary planOneQuery(File path) throws Exception {
        String query = readQuery(path);
        GraphPlanner.PlannerInstance instance = planner.instance(query, irMeta);
        GraphPlanner.Summary summary = instance.plan();
        return summary;
    }

    public void executeQueries() throws Exception {
        FileUtils.writeStringToFile(
                queryLog,
                "query_name\t\telapsed_time\t\tstatus\t\tresult\n",
                StandardCharsets.UTF_8,
                true);
        long timeout = FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs);
        File[] listFiles = queryDir.listFiles();
        Arrays.sort(listFiles, Comparator.comparing(File::getName));
        for (File file : listFiles) {
            String fileName = file.getName();
            if (fileName.endsWith(".cypher")) {
                String queryName = fileName.substring(0, fileName.length() - 7);
                try {
                    GraphPlanner.Summary summary = planOneQuery(file);
                    BigInteger queryId = BigInteger.valueOf(UUID.randomUUID().hashCode());
                    ExecutionRequest request =
                            new ExecutionRequest(
                                    queryId,
                                    "ir_plan_" + queryId,
                                    summary.getLogicalPlan(),
                                    summary.getPhysicalPlan());
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
                            new QueryTimeoutConfig(timeout),
                            new QueryLogger("", queryId));
                    StringBuilder resultBuilder = new StringBuilder();
                    while (resultIterator.hasNext()) {
                        resultBuilder.append(resultIterator.next());
                    }
                    long elapsed = System.currentTimeMillis() - startTime;
                    FileUtils.writeStringToFile(
                            queryLog,
                            queryName
                                    + "\t\t"
                                    + elapsed
                                    + "\t\t"
                                    + "success\t\t"
                                    + resultBuilder.substring(
                                            0, Math.min(resultBuilder.length(), 50))
                                    + "\n",
                            StandardCharsets.UTF_8,
                            true);
                } catch (Throwable e) {
                    FileUtils.writeStringToFile(
                            errorLog,
                            queryName + "\t\t" + e.getMessage() + "\n\n",
                            StandardCharsets.UTF_8,
                            true);
                }
            }
        }
        this.client.close();
    }

    public String readQuery(File path) throws Exception {
        String content = FileUtils.readFileToString(path, StandardCharsets.UTF_8);
        // Parse parameters
        Map<String, String> parameters = parseParameters(content);

        // Extract query template (excluding parameter declarations)
        String queryTemplate = extractQueryTemplate(content);

        // Replace parameters in the query template
        String convertedQuery = replaceParametersInQuery(queryTemplate, parameters);

        System.out.println(convertedQuery);

        return convertedQuery;
    }

    // Parses :param declarations into a Map
    public Map<String, String> parseParameters(String content) {
        Map<String, String> parameters = new HashMap<>();
        Pattern pattern = Pattern.compile(":param (\\w+) => (.+?);");
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String value = matcher.group(2).trim();

            // Handle numeric values and quoted strings
            if (value.matches("^\\d+$")) {
                parameters.put(key, value); // Numeric
            } else if (value.startsWith("\"") && value.endsWith("\"")) {
                parameters.put(key, value.substring(1, value.length() - 1)); // Quoted string
            } else {
                parameters.put(key, value); // Default
            }
        }

        return parameters;
    }

    // Extracts the Cypher query template from the file content
    public String extractQueryTemplate(String content) {
        // Remove lines starting with ':param'
        return content.replaceAll("(?m)^:param .*?;\\s*$", "").trim();
    }

    // Replaces $parameterName with actual values in the query, wrapping strings in double quotes
    public String replaceParametersInQuery(String query, Map<String, String> parameters) {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String parameterPlaceholder = "\\$" + entry.getKey();
            String value = entry.getValue();

            // Check if the value should be treated as a string
            if (!value.matches("^\\d+(\\.\\d+)?$")) { // Not numeric
                value = "\"" + value + "\""; // Wrap in double quotes
            }

            // Replace the placeholder with the value
            query = query.replaceAll(parameterPlaceholder, Matcher.quoteReplacement(value));
        }
        return query;
    }

    public static void main(String[] args) throws Exception {
        File queryDir = new File(System.getProperty("dir", "IC_QUERY_GOPT"));
        Configs configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        ICBenchTest test = new ICBenchTest(configs, queryDir);
        test.executeQueries();
    }
}
