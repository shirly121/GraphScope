package com.alibaba.graphscope.ds;

import java.lang.Integer;

public class GSVertexArray_cxx_0x2ceb3d1fFactory implements GSVertexArray.Factory<Integer> {
  public static final GSVertexArray.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new GSVertexArray_cxx_0x2ceb3d1fFactory();
  }

  public GSVertexArray_cxx_0x2ceb3d1fFactory() {
  }

  public GSVertexArray<Integer> create() {
    return new GSVertexArray_cxx_0x2ceb3d1f(GSVertexArray_cxx_0x2ceb3d1f.nativeCreateFactory0());
  }
}
