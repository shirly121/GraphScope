package com.alibaba.graphscope.ds;

import java.lang.Double;

public class GSVertexArray_cxx_0x13932289Factory implements GSVertexArray.Factory<Double> {
  public static final GSVertexArray.Factory<Double> INSTANCE;

  static {
    INSTANCE = new GSVertexArray_cxx_0x13932289Factory();
  }

  public GSVertexArray_cxx_0x13932289Factory() {
  }

  public GSVertexArray<Double> create() {
    return new GSVertexArray_cxx_0x13932289(GSVertexArray_cxx_0x13932289.nativeCreateFactory0());
  }
}
