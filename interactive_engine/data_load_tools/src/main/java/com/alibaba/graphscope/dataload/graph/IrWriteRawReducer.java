package com.alibaba.graphscope.dataload.graph;

import com.alibaba.graphscope.dataload.IrDataBuild;
import com.alibaba.graphscope.dataload.IrEdgeData;
import com.alibaba.graphscope.dataload.IrVertexData;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.volume.FileSystem;
import com.aliyun.odps.volume.Path;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class IrWriteRawReducer extends ReducerBase {
    private int taskId;
    private IrVertexData vertexData;
    private IrEdgeData edgeData;
    private BufferOutputWrapper vertexOutput;
    private BufferOutputWrapper edgeOutput;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.taskId = context.getTaskID().getInstId();

        int bufSizeMB = Integer.valueOf(context.getJobConf().get(IrDataBuild.WRITE_RAW_BUF_SIZE_MB));
        FileSystem fileSystem = context.getOutputVolumeFileSystem();
        this.vertexOutput = new BufferOutputWrapper(
                fileSystem.create(new Path(getVertexRawPath(this.taskId))),
                bufSizeMB * 1024 * 1024);
        this.edgeOutput = new BufferOutputWrapper(
                fileSystem.create(new Path(getEdgeRawPath(this.taskId))),
                bufSizeMB * 1024 * 1024);

        this.vertexData = new IrVertexData();
        this.edgeData = new IrEdgeData();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) {
        try {
            int type = key.getBigint(1).intValue();
            while (values.hasNext()) {
                Record record = values.next();
                if (type == 0) { // vertex
                    this.vertexData.readRecord(record);
                    this.vertexOutput.writeByteArray(this.vertexData.toBytes());
                } else { // edge
                    this.edgeData.readRecord(record);
                    this.edgeOutput.writeByteArray(this.edgeData.toBytes());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
        super.cleanup(context);
        this.vertexOutput.flush();
        this.edgeOutput.flush();
        this.vertexOutput.close();
        this.edgeOutput.close();
    }

    private String getVertexRawPath(int partitionId) throws IOException {
        return Paths.get("vertices" + "_" + partitionId).toString();
    }

    private String getEdgeRawPath(int partitionId) throws IOException {
        return Paths.get("edges" + "_" + partitionId).toString();
    }

    private void makePathIfNotExists(String filePath) throws IOException {
        File file = new File(filePath);
        File dir = file.getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
    }
}
