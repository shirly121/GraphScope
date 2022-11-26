package com.alibaba.graphscope.ds;

import java.lang.Integer;
import java.lang.Long;

public class VertexArray_cxx_0xd8921725Factory implements VertexArray.Factory<Long, Integer> {
  public static final VertexArray.Factory<Long, Integer> INSTANCE;

  static {
    INSTANCE = new VertexArray_cxx_0xd8921725Factory();
  }

  public VertexArray_cxx_0xd8921725Factory() {
  }

  public VertexArray<Long, Integer> create() {
    return new VertexArray_cxx_0xd8921725(VertexArray_cxx_0xd8921725.nativeCreateFactory0());
  }
}
