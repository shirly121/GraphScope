package com.alibaba.graphscope.common.ir.glogue;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ScaleTest {
    private static Session session;
    private static File logFile;
    private static int limit;

    @BeforeClass
    public static void beforeClass() throws Exception {
        String neo4jServerUrl =
                System.getProperty("neo4j.bolt.server.url", "neo4j://localhost:7687");
        session = GraphDatabase.driver(neo4jServerUrl).session();
        logFile = new File(System.getProperty("log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
        limit = Integer.valueOf(System.getProperty("limit", "10"));
    }

    @Test
    public void ldbc_1_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_1"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_1******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params = ImmutableMap.of("id", personId, "fName", "\"Mikhail\"");
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_2_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_2"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_2******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of("id", personId, "date", 20130301000000000L);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_3_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_3"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_3******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of(
                            "id",
                            personId,
                            "xName",
                            "\"Laos\"",
                            "yName",
                            "\"United_States\"",
                            "date1",
                            20100505013715278L,
                            "date2",
                            20130604130807720L);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_4_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_4"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_4******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of(
                            "id", personId,
                            "date1", 20100111014617581L,
                            "date2", 20130604130807720L);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_5_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_5"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_5******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of("id", personId, "date", 20100325000000000L);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_6_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_6"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_6******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of("id", personId, "tag", "\"North_German_Confederation\"");
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_7_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_7"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_7******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params = ImmutableMap.of("id", personId);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_8_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_8"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_8******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params = ImmutableMap.of("id", personId);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_9_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_9"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_9******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of("id", personId, "date", 20130301000000000L);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    logFile,
                    "count: " + count + ", time: " + elapsedTime + "\n",
                    StandardCharsets.UTF_8,
                    true);
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_11_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_11"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_11******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of("id", personId, "name", "\"India\"", "year", 2012);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    logFile,
                    "count: " + count + ", time: " + elapsedTime + "\n",
                    StandardCharsets.UTF_8,
                    true);
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    @Test
    public void ldbc_12_test() throws Exception {
        String template =
                FileUtils.readFileToString(
                        new File("queries/ldbc_templates/query_12"), StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(
                logFile,
                "*******************************LDBC_12******************************:\n",
                StandardCharsets.UTF_8,
                true);
        long totalElapsedTime = 0;
        int queryCount = 0;
        for (Long personId : getRandomPersonIds()) {
            Map<String, Object> params =
                    ImmutableMap.of("id", personId, "class", "\"Organisation\"");
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long count = getCount(result);
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    logFile,
                    "count: " + count + ", time: " + elapsedTime + "\n",
                    StandardCharsets.UTF_8,
                    true);
            if (count != 0) {
                ++queryCount;
                totalElapsedTime += elapsedTime;
            }
        }
        double averageTime = 0.0d;
        if (queryCount != 0) {
            averageTime = totalElapsedTime / queryCount;
        }
        FileUtils.writeStringToFile(
                logFile, "average time: " + averageTime + "\n\n\n", StandardCharsets.UTF_8, true);
    }

    private List<Long> getRandomPersonIds() {
        String query = String.format("Match (p:PERSON) Return p.id as id limit %d", limit);
        Result result = session.run(query);
        return result.list(r -> r.get("id").asLong());
    }

    private long getCount(Result result) {
        return result.next().values().get(0).asLong();
    }
}
