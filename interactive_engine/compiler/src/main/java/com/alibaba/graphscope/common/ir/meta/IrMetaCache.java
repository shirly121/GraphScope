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

package com.alibaba.graphscope.common.ir.meta;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.ir.meta.fetcher.IrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphStatistics;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaInputStream;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaSpec;
import com.alibaba.graphscope.common.ir.planner.GlogueHolder;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class IrMetaCache {
    private final LoadingCache<IrMetaCache.Key, IrMeta> cache;

    private IrMetaCache(Configs configs) {
        int cacheSize = FrontendConfig.IR_META_CACHE_SIZE.get(configs);
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .build(
                                CacheLoader.from(
                                        key -> {
                                            try {
                                                IrMetaReader reader =
                                                        new StringMetaReader(
                                                                key.schemaYaml,
                                                                key.statsJson,
                                                                key.configs);
                                                IrMetaFetcher metaFetcher =
                                                        new StaticIrMetaFetcher(reader, key.holder);
                                                return metaFetcher.fetch().get();
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }));
    }

    public IrMeta get(Key key) throws ExecutionException {
        return cache.get(key);
    }

    private static IrMetaCache instance = null;

    public static synchronized IrMetaCache getInstance(Configs configs) {
        if (instance == null) {
            instance = new IrMetaCache(configs);
        }
        return instance;
    }

    public static class Key {
        public long version;
        public Configs configs;
        public String schemaYaml;
        public String statsJson;
        public GlogueHolder holder;

        public Key(
                long version,
                Configs configs,
                String schemaYaml,
                String statsJson,
                GlogueHolder holder) {
            this.version = version;
            this.configs = configs;
            this.schemaYaml = schemaYaml;
            this.statsJson = statsJson;
            this.holder = holder;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(version);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Key other = (Key) obj;
            return this.version == other.version;
        }
    }

    static class StringMetaReader implements IrMetaReader {
        private final String schemaYaml;
        private final String statsJson;
        private final Configs configs;

        public StringMetaReader(String schemaYaml, String statsJson, Configs configs) {
            this.schemaYaml = schemaYaml;
            this.statsJson = statsJson;
            this.configs = configs;
        }

        @Override
        public IrMeta readMeta() throws IOException {
            IrGraphSchema graphSchema =
                    new IrGraphSchema(
                            configs,
                            new SchemaInputStream(
                                    new ByteArrayInputStream(
                                            schemaYaml.getBytes(StandardCharsets.UTF_8)),
                                    SchemaSpec.Type.FLEX_IN_YAML));
            return new IrMeta(
                    graphSchema,
                    new GraphStoredProcedures(
                            new ByteArrayInputStream(schemaYaml.getBytes(StandardCharsets.UTF_8)),
                            this));
        }

        @Override
        public GraphStatistics readStats(GraphId graphId) throws IOException {
            return new IrGraphStatistics(
                    new ByteArrayInputStream(statsJson.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public boolean syncStatsEnabled(GraphId graphId) throws IOException {
            return false;
        }
    }
}
