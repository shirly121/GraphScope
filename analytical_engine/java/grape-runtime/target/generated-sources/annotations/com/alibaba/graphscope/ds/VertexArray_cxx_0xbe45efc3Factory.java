package com.alibaba.graphscope.ds;

import java.lang.Double;
import java.lang.Long;

public class VertexArray_cxx_0xbe45efc3Factory implements VertexArray.Factory<Long, Double> {
  public static final VertexArray.Factory<Long, Double> INSTANCE;

  static {
    INSTANCE = new VertexArray_cxx_0xbe45efc3Factory();
  }

  public VertexArray_cxx_0xbe45efc3Factory() {
  }

  public VertexArray<Long, Double> create() {
    return new VertexArray_cxx_0xbe45efc3(VertexArray_cxx_0xbe45efc3.nativeCreateFactory0());
  }
}
