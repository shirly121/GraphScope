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

package com.alibaba.graphscope.gremlin;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.ArrayList;
import java.util.List;

public class MemLeakTest {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.valueOf(args[1]);
        int threadNum = Integer.valueOf(args[2]);
        int maxQueries = Integer.valueOf(args[3]);
        String query = args[4];
        Cluster cluster = Cluster.build().addContactPoint(host).port(port).create();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            threads.add(new Thread(() -> {
                Client client = cluster.connect();
                for (int j = 0; j < maxQueries; j++) {
                    client.submit(query);
                }
                client.close();
            }));
            threads.get(i).start();
        }
        for(Thread t : threads) {
            t.join();
        }
        cluster.close();
    }
}
