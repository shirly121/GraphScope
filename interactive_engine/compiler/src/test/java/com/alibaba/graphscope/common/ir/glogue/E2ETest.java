package com.alibaba.graphscope.common.ir.glogue;

import com.google.common.base.Preconditions;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class E2ETest {
    private static Session session;
    private static File logFile;

    @BeforeClass
    public static void beforeClass() throws Exception {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
        logFile = new File(System.getProperty("client.log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
    }

    @Test
    public void e2e_test() throws Exception {
        File queryDir = new File(System.getProperty("query.dir"));
        Preconditions.checkArgument(
                queryDir.exists() && queryDir.isDirectory(),
                queryDir + " is not a valid directory");
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            String queryName = file.getName();
            String query = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            try {
                long startTime = System.currentTimeMillis();
                Result result = session.run(query);
                long e2eTime = System.currentTimeMillis() - startTime;
                FileUtils.write(
                        logFile,
                        String.format(
                                "queryName: %s, query: %s, e2e time: %d ms, result: %s\n\n\n",
                                queryName, query, e2eTime, result.list().toString()),
                        StandardCharsets.UTF_8,
                        true);
            } catch (Exception e) {
                FileUtils.write(
                        logFile,
                        String.format(
                                "queryName: %s, query: %s, error: %s\n\n\n",
                                queryName, query, e.getMessage()),
                        StandardCharsets.UTF_8,
                        true);
            }
        }
    }
}
