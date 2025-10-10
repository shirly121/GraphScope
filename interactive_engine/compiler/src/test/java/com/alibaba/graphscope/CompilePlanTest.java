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

package com.alibaba.graphscope;

import com.alibaba.graphscope.sdk.GraphPlan;
import com.alibaba.graphscope.sdk.PlanUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompilePlanTest {
    @Test
    public void compile_plan_test() throws Exception {
        int qps = Integer.valueOf(System.getProperty("qps", "50"));
        String configPath =
                System.getProperty("config", "src/test/resources/interactive_config.yaml");
        String schemaPath =
                System.getProperty(
                        "schema",
                        "src/test/resources/o11y-integration-cn-hongkong__cypher_schema.yaml");
        String statsPath =
                System.getProperty(
                        "stats",
                        "src/test/resources/o11y-integration-cn-hongkong__cypher_statistics.json");
        String schemaYaml =
                FileUtils.readFileToString(new File(schemaPath), StandardCharsets.UTF_8);
        String statsJson = FileUtils.readFileToString(new File(statsPath), StandardCharsets.UTF_8);
        String queryPath = System.getProperty("queries", "src/test/resources/queries");
        List<String> queries = Files.readAllLines(Paths.get(queryPath));
        if (queries.isEmpty()) {
            throw new RuntimeException("no queries to compile");
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1); // 只用来定时调度
        ExecutorService queryPool = Executors.newFixedThreadPool(5); // 真实并发执行池，大小可调

        AtomicInteger pos = new AtomicInteger(0);
        AtomicInteger statsCount = new AtomicInteger(0);
        long lastStatsTime = System.currentTimeMillis();

        AtomicInteger version = new AtomicInteger(0);

        Runnable submitTasks =
                () -> {
                    queryPool.submit(
                            () -> {
                                String query = queries.get(pos.getAndIncrement() % queries.size());
                                statsCount.getAndIncrement();
                                GraphPlan plan =
                                        PlanUtils.compilePlan(
                                                configPath,
                                                query,
                                                version.getAndIncrement(),
                                                schemaYaml,
                                                statsJson);
                                if (version.get() >= 10) {
                                    version.set(0);
                                }
                                if (!"OK".equals(plan.errorCode)) {
                                    throw new RuntimeException("failed to compile...");
                                }
                                // System.out.println("query plan is ok");
                            });
                };

        long intervalMillis = 1000 / qps;
        scheduler.scheduleAtFixedRate(submitTasks, 0, intervalMillis, TimeUnit.MILLISECONDS);

        while (true) {
            long now = System.currentTimeMillis();
            if (now - lastStatsTime >= 1000) {
                System.out.printf("Actual QPS: %d\n", statsCount.get());
                statsCount.set(0);
                lastStatsTime = now;
            }
        }
    }
}
