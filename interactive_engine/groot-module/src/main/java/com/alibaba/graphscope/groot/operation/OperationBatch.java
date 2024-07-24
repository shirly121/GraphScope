/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.operation;

import com.alibaba.graphscope.proto.groot.OperationBatchPb;
import com.alibaba.graphscope.proto.groot.OperationPb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class OperationBatch implements Iterable<OperationBlob> {

    private final long latestSnapshotId;
    private final List<OperationBlob> operationBlobs;
    private final String traceId;

    private OperationBatch(
            long latestSnapshotId, List<OperationBlob> operationBlobs, String traceId) {
        this.latestSnapshotId = latestSnapshotId;
        this.operationBlobs = operationBlobs;
        this.traceId = traceId;
    }

    public static OperationBatch parseProto(OperationBatchPb proto) {
        long latestSnapshotId = proto.getLatestSnapshotId();
        List<OperationPb> operationPbs = proto.getOperationsList();
        List<OperationBlob> operationBlobs = new ArrayList<>(operationPbs.size());
        for (OperationPb operationPb : operationPbs) {
            operationBlobs.add(OperationBlob.parseProto(operationPb));
        }
        return new OperationBatch(latestSnapshotId, operationBlobs, proto.getTraceId());
    }

    public int getOperationCount() {
        return operationBlobs.size();
    }

    @Override
    public Iterator<OperationBlob> iterator() {
        return operationBlobs.iterator();
    }

    public long getLatestSnapshotId() {
        return latestSnapshotId;
    }

    public String getTraceId() {
        return traceId;
    }

    public OperationBlob getOperationBlob(int i) {
        return operationBlobs.get(i);
    }

    public OperationBatchPb toProto() {
        OperationBatchPb.Builder builder = OperationBatchPb.newBuilder();
        builder.setLatestSnapshotId(latestSnapshotId);
        if (this.traceId != null) {
            builder.setTraceId(traceId);
        }
        for (OperationBlob operationBlob : operationBlobs) {
            builder.addOperations(operationBlob.toProto());
        }
        return builder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(List<Operation> operations) {
        return new Builder(operations);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationBatch that = (OperationBatch) o;

        return Objects.equals(operationBlobs, that.operationBlobs);
    }

    public static class Builder {

        private boolean built = false;
        private long latestSnapshotId;
        private String traceId;
        private List<OperationBlob> operationBlobs;

        private Builder() {
            this.latestSnapshotId = 0L;
            this.traceId = null;
            this.operationBlobs = new ArrayList<>();
        }

        private Builder(List<Operation> operations) {
            this();
            for (Operation operation : operations) {
                addOperation(operation);
            }
        }

        public Builder addOperation(Operation operation) {
            return addOperationBlob(operation.toBlob());
        }

        public Builder addOperationBlob(OperationBlob operationBlob) {
            if (this.built) {
                throw new IllegalStateException("cannot add operation after built");
            }
            this.operationBlobs.add(operationBlob);
            return this;
        }

        public Builder setLatestSnapshotId(long latestSnapshotId) {
            this.latestSnapshotId = latestSnapshotId;
            return this;
        }

        public Builder setTraceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public OperationBatch build() {
            this.built = true;
            return new OperationBatch(latestSnapshotId, operationBlobs, traceId);
        }
    }
}
