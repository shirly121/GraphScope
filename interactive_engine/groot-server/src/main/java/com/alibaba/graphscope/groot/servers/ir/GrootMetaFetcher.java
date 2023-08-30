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

package com.alibaba.graphscope.groot.servers.ir;

import com.alibaba.graphscope.common.ir.meta.schema.GraphSchemaWrapper;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.store.IrMetaFetcher;
import com.alibaba.graphscope.common.store.SnapshotId;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.SchemaFetcher;
import com.alibaba.graphscope.groot.common.util.IrSchemaParser;

import java.io.IOException;
import java.util.*;

public class GrootMetaFetcher implements IrMetaFetcher {
    private IrSchemaParser parser;
    private SchemaFetcher schemaFetcher;

    public GrootMetaFetcher(SchemaFetcher schemaFetcher) {
        this.parser = IrSchemaParser.getInstance();
        this.schemaFetcher = schemaFetcher;
    }

    @Override
    public Optional<IrMeta> fetch() {
        Map<Long, GraphSchema> pair = this.schemaFetcher.getSchemaSnapshotPair();

        if (!pair.isEmpty()) {
            Map.Entry<Long, GraphSchema> entry = pair.entrySet().iterator().next();
            Long snapshotId = entry.getKey();
            GraphSchema schema = entry.getValue();

            try {
                return Optional.of(
                        new IrMeta(
                                new SnapshotId(true, snapshotId),
                                new GraphSchemaWrapper(schema, parser.parse(schema), true)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return Optional.empty();
        }
    }
}
