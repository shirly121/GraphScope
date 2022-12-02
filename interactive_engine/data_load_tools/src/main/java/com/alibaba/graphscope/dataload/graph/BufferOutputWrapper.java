package com.alibaba.graphscope.dataload.graph;

import java.io.DataOutputStream;
import java.io.IOException;

public class BufferOutputWrapper {
    private DataOutputStream output;
    private byte[] buffer;
    private int len;
    private int capacity;

    public BufferOutputWrapper(DataOutputStream output, int capacity) {
        this.output = output;
        this.capacity = capacity;
        this.buffer = new byte[capacity];
    }

    public void writeByteArray(byte[] bytes) throws IOException {
        if (this.len + bytes.length > this.capacity) {
            this.output.write(this.buffer, 0, this.len);
            this.len = 0;
        }
        System.arraycopy(bytes, 0, this.buffer, this.len, bytes.length);
        this.len += bytes.length;
    }

    public void flush() throws IOException {
        if (this.len > 0) {
            this.output.write(this.buffer, 0, this.len);
        }
    }

    public void close() throws IOException {
        if (this.output != null) {
            this.output.close();
        }
    }
}
