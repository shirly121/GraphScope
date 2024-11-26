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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class BIBenchTest1 {
    @Test
    public void BI_Bench_Test() throws Exception {
        File queryDir = new File(System.getProperty("dir", "BI_QUERY_GOPT"));
        File queryLog = new File(Path.of(queryDir.getAbsolutePath(), "query.log").toString());
        Configs configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        ICBenchTest test = new ICBenchTest(configs, queryDir, queryLog);
        test.executeQueries();
    }

    @Test
    public void execute_one_query() throws Exception {
        File queryDir = new File(System.getProperty("dir", "BI_QUERY_GOPT"));
        File queryLog = new File(Path.of(queryDir.getAbsolutePath(), "query.log").toString());
        Configs configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        String queryPath = "IC_QUERY_GOPT/IC_13.cypher";
        ICBenchTest benchTest = new ICBenchTest(configs, queryDir, queryLog);
        GraphPlanner.Summary summary = benchTest.planOneQuery(new File(queryPath));
        System.out.println(summary.getLogicalPlan().explain());
        System.out.println(summary.getPhysicalPlan().explain());
    }
}
