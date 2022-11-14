package com.alibaba.graphscope.dataload.graph;

import com.alibaba.graphscope.dataload.IrDataBuild;
import com.alibaba.graphscope.dataload.IrEdgeData;
import com.alibaba.graphscope.dataload.IrVertexData;
import com.alibaba.maxgraph.dataload.OSSFileObj;
import com.alibaba.maxgraph.dataload.databuild.OfflineBuildOdps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class IrWriteRawReducer extends ReducerBase {
    private String localRootDir = "/tmp";
    private int taskId;
    private OSSFileObj ossFileObj;
    private String ossBucketName;
    private String ossObjectPrefix;
    private IrVertexData vertexData;
    private IrEdgeData edgeData;
    private OutputStream vertexOutput;
    private OutputStream edgeOutput;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.taskId = context.getTaskID().getInstId();
        this.ossBucketName = context.getJobConf().get(OfflineBuildOdps.OSS_BUCKET_NAME);
        this.ossObjectPrefix = context.getJobConf().get(IrDataBuild.WRITE_GRAPH_OSS_PATH);

        String ossAccessId = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_ID);
        String ossAccessKey = context.getJobConf().get(OfflineBuildOdps.OSS_ACCESS_KEY);
        String ossEndPoint = context.getJobConf().get(OfflineBuildOdps.OSS_ENDPOINT);

        Map<String, String> ossInfo = new HashMap();
        ossInfo.put(OfflineBuildOdps.OSS_ENDPOINT, ossEndPoint);
        ossInfo.put(OfflineBuildOdps.OSS_ACCESS_ID, ossAccessId);
        ossInfo.put(OfflineBuildOdps.OSS_ACCESS_KEY, ossAccessKey);
        this.ossFileObj = new OSSFileObj(ossInfo);

        String vertexFullPath = Paths.get(this.localRootDir, getVertexRawPath(taskId)).toString();
        String edgeFullPath = Paths.get(this.localRootDir, getEdgeRawPath(taskId)).toString();
        makePathIfNotExists(vertexFullPath);
        makePathIfNotExists(edgeFullPath);

        this.vertexOutput =
                new BufferedOutputStream(
                        new FileOutputStream(vertexFullPath, true), 64 * 1024 * 1024);
        this.edgeOutput =
                new BufferedOutputStream(
                        new FileOutputStream(edgeFullPath, true), 64 * 1024 * 1024);
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
                    this.vertexOutput.write(this.vertexData.toBytes());
                } else { // edge
                    this.edgeData.readRecord(record);
                    this.edgeOutput.write(this.edgeData.toBytes());
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
        List<String> rawPaths =
                Arrays.asList(getVertexRawPath(this.taskId), getEdgeRawPath(this.taskId));
        for (String rawPath : rawPaths) {
            String ossObjectName = Paths.get(this.ossObjectPrefix, rawPath).toString();
            String localPath = Paths.get(this.localRootDir, rawPath).toString();
            this.ossFileObj.uploadFileWithCheckPoint(
                    this.ossBucketName, ossObjectName, new File(localPath));
        }
        this.ossFileObj.close();
        File file = new File(Paths.get(localRootDir, "raw_data").toString());
        if (file.exists() && file.isDirectory()) {
            file.delete();
        }
    }

    private String getVertexRawPath(int partitionId) throws IOException {
        String dir = "raw_data";
        String partition = "partition_" + partitionId;
        return Paths.get(dir, partition, "vertices").toString();
    }

    private String getEdgeRawPath(int partitionId) throws IOException {
        String dir = "raw_data";
        String partition = "partition_" + partitionId;
        return Paths.get(dir, partition, "edges").toString();
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
