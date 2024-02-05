package com.alibaba.graphscope.common.ir.glogue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PathQueryTest {
    private static Cluster cluster;
    private static Client client;
    private static File queryDir;
    private static File logFile;
    private static int limit;

    @BeforeClass
    public static void beforeClass() throws Exception {
        int port = Integer.valueOf(System.getProperty("port", "8182"));
        cluster = Cluster.build().addContactPoint("localhost").port(port).create();
        client = cluster.connect();
        queryDir = new File(System.getProperty("query"));
        logFile = new File(System.getProperty("log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
        limit = Integer.valueOf(System.getProperty("limit", "16"));
    }

    @Test
    public void run_path_test() throws Exception {
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            String queryName = file.getName();
            String template = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids);
            template = StringSubstitutor.replace(template, params, "$_", "_");
            for (int workNum = 32; workNum >= 2; workNum /= 2) {
                Map<String, Object> params1 = ImmutableMap.of("workNum", workNum);
                String query = StringSubstitutor.replace(template, params1, "$_", "_");
                FileUtils.writeStringToFile(
                        logFile,
                        String.format("%s: %s\n", queryName, query),
                        StandardCharsets.UTF_8,
                        true);
                try {
                    long start = System.currentTimeMillis();
                    ResultSet resultSet = client.submit(query);
                    StringBuilder resultBuilder = new StringBuilder();
                    for (Result result : resultSet) {
                        resultBuilder.append(result);
                    }
                    long elapsed = System.currentTimeMillis() - start;
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format("elapsed: %dms, result %s\n", elapsed, resultBuilder),
                            StandardCharsets.UTF_8,
                            true);
                } catch (Exception e) {
                    FileUtils.writeStringToFile(
                            logFile,
                            String.format("failed: %s\n", e.getMessage()),
                            StandardCharsets.UTF_8,
                            true);
                }
                FileUtils.writeStringToFile(
                        logFile, String.format("\n\n\n"), StandardCharsets.UTF_8, true);
            }
        }
    }

    private List<Long> getRandomPersonIds() {
        String query = String.format("g.V().hasLabel('PERSON').limit(%d).values('id')", limit);
        ResultSet results = client.submit(query);
        List<Long> ids = Lists.newArrayList();
        for (Result result : results) {
            ids.add(result.getLong());
        }
        return ids;
    }

    @AfterClass
    public static void afterClass() {
        client.close();
        cluster.close();
    }
}
