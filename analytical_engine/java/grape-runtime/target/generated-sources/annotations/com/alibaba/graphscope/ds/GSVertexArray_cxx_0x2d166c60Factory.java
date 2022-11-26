package com.alibaba.graphscope.ds;

import java.lang.Long;

public class GSVertexArray_cxx_0x2d166c60Factory implements GSVertexArray.Factory<Long> {
  public static final GSVertexArray.Factory<Long> INSTANCE;

  static {
    INSTANCE = new GSVertexArray_cxx_0x2d166c60Factory();
  }

  public GSVertexArray_cxx_0x2d166c60Factory() {
  }

  public GSVertexArray<Long> create() {
    return new GSVertexArray_cxx_0x2d166c60(GSVertexArray_cxx_0x2d166c60.nativeCreateFactory0());
  }
}
