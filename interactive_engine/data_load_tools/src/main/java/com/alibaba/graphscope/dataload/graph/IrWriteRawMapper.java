package com.alibaba.graphscope.dataload.graph;

import com.alibaba.graphscope.dataload.IrDataBuild;
import com.alibaba.graphscope.dataload.IrEdgeData;
import com.alibaba.graphscope.dataload.IrVertexData;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

import java.io.IOException;

public class IrWriteRawMapper extends MapperBase {
    private Record key;
    private int partitions;
    private int reducerNum;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.key = context.createMapOutputKeyRecord();
        this.partitions =
                Integer.valueOf(context.getJobConf().get(IrDataBuild.WRITE_PARTITION_NUM));
        this.reducerNum = Integer.valueOf(context.getJobConf().get(IrDataBuild.WRITE_REDUCER_NUM));
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String tableName = context.getInputTableInfo().getTableName();
        if (tableName.contains(IrDataBuild.ENCODE_VERTEX_MAGIC)) { // vertex
            IrVertexData vertexData = new IrVertexData();
            vertexData.readRecord(record);
            this.key.setBigint(0, vertexData.id);
            this.key.setBigint(1, 0L); // 0 -> vertex type
            context.write(this.key, record);
        } else { // edge
            IrEdgeData edgeData = new IrEdgeData();
            edgeData.readRecord(record);
            // write edges according to src vertex
            this.key.setBigint(0, edgeData.srcVertexId);
            this.key.setBigint(1, 1L); // 1 -> edge type
            context.write(this.key, record);
            if (!isSrcDstSamePartition(edgeData)) {
                // write edges according to dst vertex
                this.key.setBigint(0, edgeData.dstVertexId);
                this.key.setBigint(1, 1L); // 1 -> edge type
                context.write(this.key, record);
            }
        }
    }

    private boolean isSrcDstSamePartition(IrEdgeData edgeData) {
        long srcId = edgeData.srcVertexId;
        long dstId = edgeData.dstVertexId;
        return PartitionUtils.getVertexPartition(srcId, partitions, reducerNum)
                == PartitionUtils.getVertexPartition(dstId, partitions, reducerNum);
    }
}
