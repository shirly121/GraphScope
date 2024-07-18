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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.IrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class CompileTimeTest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;
    private static File output;

    @Before
    public void beforeTest() throws Exception {
        String configFile = "conf/ir.compiler.properties";
        configs =
                new Configs(configFile);
        optimizer = new GraphRelOptimizer(configs);
        IrMetaFetcher metaFetcher = new StaticIrMetaFetcher(new LocalIrMetaReader(configs), optimizer.getGlogueHolder());
        irMeta = metaFetcher.fetch().get();
        output = new File("compile.log");
        if (output.exists()) {
            output.delete();
        }
    }

    @Test
    public void testCompileTime() throws Exception {
        File file = new File("queries");
        for (File f : file.listFiles()) {
            String queryName = f.getName();
            String query = FileUtils.readFileToString(f, StandardCharsets.UTF_8);
            GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
            RelNode before =
                    com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder)
                            .build();
            long startTime = System.currentTimeMillis();
            RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
            long elapsedTime = System.currentTimeMillis() - startTime;
            FileUtils.writeStringToFile(output, "queryName: [" + queryName + "]; compile time: [" + elapsedTime + "] ms\n", StandardCharsets.UTF_8, true);
            FileUtils.writeStringToFile(output, after.explain(), StandardCharsets.UTF_8, true);
            FileUtils.writeStringToFile(output, "\n\n\n", StandardCharsets.UTF_8, true);
        }
    }
}
