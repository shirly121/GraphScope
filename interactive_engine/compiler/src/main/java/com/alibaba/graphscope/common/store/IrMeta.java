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

package com.alibaba.graphscope.common.store;

public class IrMeta {
    private long snapshotId;
    private String schema;
    private String catalogPath;
    private boolean acquireSnapshot;

    public IrMeta(String schema) {
        this.schema = schema;
        this.acquireSnapshot = false;
    }

    public IrMeta(String schema, String catalogPath) {
        this.schema = schema;
        this.catalogPath = catalogPath;
    }

    public IrMeta(String schema, long snapshotId) {
        this.schema = schema;
        this.snapshotId = snapshotId;
        this.acquireSnapshot = true;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public String getSchema() {
        return schema;
    }

    public String getCatalogPath() {
        return catalogPath;
    }

    public boolean isAcquireSnapshot() {
        return acquireSnapshot;
    }
}
