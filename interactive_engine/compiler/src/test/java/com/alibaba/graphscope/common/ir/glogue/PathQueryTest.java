package com.alibaba.graphscope.common.ir.glogue;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class PathQueryTest {
    private static Cluster cluster;
    private static Client client;
    private static File queryDir;
    private static File logFile;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        client = cluster.connect();
        queryDir = new File(System.getProperty("query"));
        logFile = new File(System.getProperty("log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
    }

    @Test
    public void run_path_test() throws Exception {
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            String queryName = file.getName();
            String query = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
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

    @AfterClass
    public static void afterClass() {
        client.close();
        cluster.close();
    }
}
