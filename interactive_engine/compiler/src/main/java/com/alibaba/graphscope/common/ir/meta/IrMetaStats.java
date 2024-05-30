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

package com.alibaba.graphscope.common.ir.meta;

import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;

import org.checkerframework.checker.nullness.qual.Nullable;

public class IrMetaStats extends IrMeta {
    private final @Nullable GraphStatistics statistics;

    public IrMetaStats(
            SnapshotId snapshotId,
            IrGraphSchema schema,
            GraphStoredProcedures storedProcedures,
            GraphStatistics statistics) {
        super(snapshotId, schema, storedProcedures);
        this.statistics = statistics;
    }

    public @Nullable GraphStatistics getStatistics() {
        return this.statistics;
    }
}
