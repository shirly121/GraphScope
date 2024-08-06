package com.alibaba.graphscope.gaia.clients.gremlin;

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphResultSet;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GremlinGraphClient implements GraphClient {
    private final Client client;
    private static Logger logger = LoggerFactory.getLogger(GremlinGraphClient.class);

    public GremlinGraphClient(String endpoint, String username, String password) {
        String[] address = endpoint.split(":");
        Cluster.Builder cluster =
                Cluster.build()
                        .addContactPoint(address[0])
                        .port(Integer.parseInt(address[1]))
                        .serializer(initializeSerialize());
        if (!(username == null || username.isEmpty())
                && !(password == null || password.isEmpty())) {
            cluster.credentials(username, password);
        }
        Client gremlinClient = cluster.create().connect();
        if (gremlinClient == null) {
            throw new RuntimeException("Failed to create client with gremlin server");
        }
        this.client = gremlinClient;
        logger.info("Connected to gremlin server at " + endpoint);
    }

    @Override
    public GraphResultSet submit(String query) {
        ResultSet gremlinResultSet = client.submit(query);
        return new GremlinGraphResultSet(gremlinResultSet);
    }

    @Override
    public void close() {
        client.close();
    }

    private static MessageSerializer initializeSerialize() {
        return new GryoMessageSerializerV1d0();
    }
}
