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

import java.io.File;

public class BIBenchTest {
    public static void main(String[] args) throws Exception {
        File queryDir = new File(System.getProperty("dir", "BI_QUERY_GOPT"));
        Configs configs = new Configs(System.getProperty("config", "conf/ir.compiler.properties"));
        ICBenchTest test = new ICBenchTest(configs, queryDir);
        test.executeQueries();
    }
}
