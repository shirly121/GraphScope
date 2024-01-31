package org.apache.calcite.plan.volcano;

import com.alibaba.graphscope.common.config.AuthConfig;
import com.alibaba.graphscope.common.config.Configs;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class STPathTest2 {
    public static void main(String[] args) throws Exception {
        Configs configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        File queryDir = new File(System.getProperty("query.dir", "queries_bak"));
        File logFile = new File(System.getProperty("log", "log"));
        if (logFile.exists()) {
            logFile.delete();
        }
        logFile.createNewFile();
        int gremlinPort = Integer.valueOf(System.getProperty("gremlin.port", "8182"));
        AuthProperties authProperties = new AuthProperties();
        authProperties
                .with(AuthProperties.Property.USERNAME, AuthConfig.AUTH_USERNAME.get(configs))
                .with(AuthProperties.Property.PASSWORD, AuthConfig.AUTH_PASSWORD.get(configs));
        Cluster cluster =
                Cluster.build()
                        .addContactPoint("localhost")
                        .port(gremlinPort)
                        .authProperties(authProperties)
                        .create();
        Client client = cluster.connect();
        List<File> files = Arrays.asList(queryDir.listFiles());
        Collections.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            long startTime = System.currentTimeMillis();
            String query = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            FileUtils.writeStringToFile(logFile, query + "\n", StandardCharsets.UTF_8, true);
            ResultSet results = client.submit(query);
            StringBuilder sb = new StringBuilder();
            for (Result result : results) {
                sb.append(result.getObject().toString());
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(
                    logFile,
                    file.getName() + ", execution time: " + elapsedTime + ", count: " + sb + "\n",
                    StandardCharsets.UTF_8,
                    true);
        }
    }
}
