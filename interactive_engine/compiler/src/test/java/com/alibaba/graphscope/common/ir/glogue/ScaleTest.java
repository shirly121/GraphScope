package com.alibaba.graphscope.common.ir.glogue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_1"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_1******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            String fName = "\"Mikhail\"";
            List<Long> randomIds = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", randomIds, "fName", fName);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    logFile,
                    "count: " + getCount(result) + ", time: " + elapsedTime + "\n",
                    StandardCharsets.UTF_8,
                    true);
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_2_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_2"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_2******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            List<Long> randomIds = getRandomPersonIds();
            Map<String, Object> params =
                    ImmutableMap.of("id", randomIds, "date", 20130301000000000L);
            String query = StringSubstitutor.replace(template, params, "$_", "_");
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            long startTime = System.currentTimeMillis();
            Result result = session.run(query);
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    logFile,
                    "count: " + getCount(result) + ", time: " + elapsedTime + "\n",
                    StandardCharsets.UTF_8,
                    true);
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_3_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_3"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_3******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_4_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_4"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_4******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params =
                    ImmutableMap.of(
                            "id", ids,
                            "date1", 20100111014617581L,
                            "date2", 20130604130807720L);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_5_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_5"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_5******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids, "date", 20100325000000000L);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_6_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_6"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_6******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            String tagName = "\"Augustine_of_Hippo\"";
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids, "tag", tagName);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_7_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_7"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_7******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_8_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_8"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_8******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_9_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_9"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_9******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids, "date", 20130301000000000L);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_11_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_11"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_11******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            String country = "\"United_States\"";
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids, "name", country, "year", 2012);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }

    @Test
    public void ldbc_12_test() throws Exception {
        try {
            String template =
                    FileUtils.readFileToString(
                            new File("queries/ldbc_templates/query_12"), StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(
                    logFile,
                    "*******************************LDBC_12******************************:\n",
                    StandardCharsets.UTF_8,
                    true);
            String className = "\"Album\"";
            List<Long> ids = getRandomPersonIds();
            Map<String, Object> params = ImmutableMap.of("id", ids, "class", className);
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
        } catch (Exception e) {
            FileUtils.writeStringToFile(
                    logFile, "error: " + e.getMessage() + "\n\n\n", StandardCharsets.UTF_8, true);
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
    }

    //    private List<Long> getPersonsInCountries(String countryX, String countryY) {
    //        String query =
    //                String.format(
    //                        "Match"
    //                            + "
    // (countryX:COUNTRY)<-[:ISLOCATEDIN]-(messageX)-[:HASCREATOR]->(otherP:PERSON)-[:KNOWS]-(p:PERSON)"
    //                            + " Where countryX.name IN [%s, %s] Return p.id as id Limit %d",
    //                        countryX, countryY, limit);
    //        Result result = session.run(query);
    //        List<Long> ids = Lists.newArrayList();
    //        while (result.hasNext()) {
    //            ids.add(result.next().get("id").asLong());
    //        }
    //        return ids;
    //    }
    //
    //    private List<Long> getPersonsOfTag(String tagName) {
    //        String query =
    //                String.format(
    //                        "MATCH"
    //                            + "
    // (person:PERSON)-[:KNOWS]-(other)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag:TAG"
    //                            + " {name: %s}) Return person.id as id Limit %d",
    //                        tagName, limit);
    //        Result result = session.run(query);
    //        List<Long> ids = Lists.newArrayList();
    //        while (result.hasNext()) {
    //            ids.add(result.next().get("id").asLong());
    //        }
    //        return ids;
    //    }
    //
    //    private List<Long> getPersonsInCountry(String country) {
    //        String query =
    //                String.format(
    //                        "MATCH"
    //                            + "
    // (person:PERSON)-[:KNOWS]-(friend:PERSON)-[workAt:WORKAT]->(company:COMPANY)-[:ISLOCATEDIN]->(:COUNTRY"
    //                            + " {name: %s}) Return person.id as id Limit %d",
    //                        country, limit);
    //        Result result = session.run(query);
    //        List<Long> ids = Lists.newArrayList();
    //        while (result.hasNext()) {
    //            ids.add(result.next().get("id").asLong());
    //        }
    //        return ids;
    //    }
    //
    //    private List<Long> getPersonsOfTagClass(String className) {
    //        String query =
    //                String.format(
    //                        "MATCH"
    //                            + "
    // (p:PERSON)-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(comment:COMMENT)-[:REPLYOF]->(:POST)-[:HASTAG]->(tag:TAG)-[:HASTYPE]->(:TAGCLASS)-[:ISSUBCLASSOF]->(baseTagClass:TAGCLASS"
    //                            + " {name: %s}) Return p.id as id Limit %d",
    //                        className, limit);
    //        Result result = session.run(query);
    //        List<Long> ids = Lists.newArrayList();
    //        while (result.hasNext()) {
    //            ids.add(result.next().get("id").asLong());
    //        }
    //        return ids;
    //    }
    //
    //    private List<Long> getPersonsOfFriend(String name) {
    //        String query =
    //                String.format(
    //                        "Match (p:PERSON)-[:KNOWS]-(f:PERSON {firstName: %s}) Return p.id as
    // id Limit"
    //                            + " %d",
    //                        name, limit);
    //        Result result = session.run(query);
    //        List<Long> ids = Lists.newArrayList();
    //        while (result.hasNext()) {
    //            ids.add(result.next().get("id").asLong());
    //        }
    //        return ids;
    //    }

    private long getCount(Result result) {
        return result.next().values().get(0).asLong();
    }
}
